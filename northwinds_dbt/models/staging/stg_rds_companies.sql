
WITH source AS (
    SELECT 
        company_name,
        country, 
        address,
        city,
        postal_code
    FROM {{ source('rds', 'customers') }}
),
-- Rename and format specific columns for clarity, including customer ID and contact names
renamed AS (
    SELECT
        CONCAT('rds-', replace(lower(company_name), ' ', '-')) AS create_company_id,
        company_name,
        max(address) as address,
        max(city)    as city,
        max(postal_code) as postal_code,
        max(country)     as country
    FROM source 
    GROUP BY 
        company_name
)

-- Final selection from the transformed data, ensuring column names match descriptions
SELECT 
    create_company_id AS company_id,
    company_name as name, 
    address, 
    city,
    postal_code,
    country
FROM renamed
