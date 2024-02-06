WITH source_contracts AS (
    SELECT 
        hubspot_id,
        first_name,
        last_name,
        phone,
        business_name
    FROM 
        dev.northwinds_hubspot
),

stg_hubspot_companies AS (
    SELECT 
        company_id,
        business_name 
    FROM 
        {{ ref('stg_hubspot_companies') }}
),

add_company_id_to_contracts AS (
    SELECT
        source_contracts.hubspot_id,
        source_contracts.first_name,
        source_contracts.last_name,
        source_contracts.phone,
        stg_hubspot_companies.company_id
    FROM source_contracts
    JOIN stg_hubspot_companies
        ON source_contracts.business_name = stg_hubspot_companies.business_name
),

cleanup_columns AS (
    SELECT
        CONCAT('hubspot-', hubspot_id) AS hubspot_id_modified, 
        first_name,
        last_name,
        REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') AS updated_phone,
        company_id 
    FROM 
        add_company_id_to_contracts
), 

final AS (
    SELECT 
        hubspot_id_modified as contact_id,
        first_name,
        last_name,
        CASE 
            WHEN LENGTH(updated_phone) = 10 THEN '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
                                                        SUBSTRING(updated_phone, 4, 3) || '-' ||
                                                        SUBSTRING(updated_phone, 7, 4) 
            END AS phone,
        company_id
    FROM cleanup_columns 
)

SELECT * FROM final

