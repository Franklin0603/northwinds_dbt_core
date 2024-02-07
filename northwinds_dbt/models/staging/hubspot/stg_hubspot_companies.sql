WITH source AS (
    SELECT 
        business_name
    FROM 
        dev.northwinds_hubspot
),
create_company_id AS (
    SELECT
        CONCAT('hubspot-', REPLACE(LOWER(business_name), ' ', '-')) AS create_company_id,
        business_name
    FROM
        source
),
final AS (
    SELECT 
        create_company_id AS company_id,
        business_name AS name
    FROM create_company_id
)

SELECT * FROM final