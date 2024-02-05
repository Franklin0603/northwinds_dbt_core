-- This dbt query transforms customer data from the 'rds' source by prepending 'rds-' to the customer_id, 
-- indicating its source origin. It also splits the contact_name into first_name and last_name, assigning 'Unknown' 
-- for any missing last names, and retains the country information. These transformations enhance the clarity of 
-- customer identification and facilitate more effective data segmentation for downstream analysis.


WITH source_customers AS (
    SELECT 
        customer_id,
        country, 
        phone,
        contact_name,
        company_name
    FROM {{ source('rds', 'customers') }}
),

stg_companies as (
    SELECT 
        name,
        company_id 
    FROM 
        ref('stg_rds_companies')
),

-- Rename and format specific columns for clarity, including customer ID and contact names
renamed AS (
    SELECT
        CONCAT('rds-', customer_id) AS customer_id_modified, 
        country,
        -- Splitting contact_name into first and last names, with handling for missing last names
        SPLIT_PART(contact_name, ' ', 1) AS first_name,
        COALESCE(NULLIF(SPLIT_PART(contact_name, ' ', 2), ''), 'Unknown') AS last_name,
        REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') AS updated_phone,
        company_id
    FROM source_customers
    JOIN stg_companies 
        ON source_customers.company_name = stg_companies.name 
), 

-- Final selection from the transformed data, ensuring column names match descriptions
final as (
SELECT 
    customer_id_modified AS customer_id, 
    first_name, 
    last_name,
    CASE 
        WHEN LENGTH(updated_phone) = 10 THEN '(' || SUBSTRING(updated_phone, 1, 3) || ') ' || 
                                                    SUBSTRING(updated_phone, 4, 3) || '-' ||
                                                    SUBSTRING(updated_phone, 7, 4) 
        END AS phone,
    company_id
FROM renamed 
)

SELECT * FROM final


