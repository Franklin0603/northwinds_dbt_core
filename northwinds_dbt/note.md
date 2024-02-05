{# This dbt query transforms customer data from the 'rds' source by prepending 'rds-' to the customer_id, indicating its source origin. It also splits the contact_name into first_name and last_name, assigning 'Unknown' for any missing last names, and retains the country information. These transformations enhance the clarity of customer identification and facilitate more effective data segmentation for downstream analysis. #}

WITH source AS (
    SELECT 
        customer_id,
        country, 
        contact_name 
    FROM {{ source('rds', 'customers') }}
),
-- Rename and format specific columns for clarity, including customer ID and contact names
renamed AS (
    SELECT
        CONCAT('rds-', customer_id) AS customer_id_modified, 
        country,
        -- Splitting contact_name into first and last names, with handling for missing last names
        SPLIT_PART(contact_name, ' ', 1) AS first_name,
        COALESCE(NULLIF(SPLIT_PART(contact_name, ' ', 2), ''), 'Unknown') AS last_name
    FROM source 
)

-- Final selection from the transformed data, ensuring column names match descriptions
SELECT 
    customer_id_modified AS customer_id,
    country, 
    first_name, 
    last_name
FROM renamed
