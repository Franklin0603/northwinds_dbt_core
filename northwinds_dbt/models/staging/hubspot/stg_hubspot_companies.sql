WITH source AS (
    SELECT 
        business_name
    FROM 
        dev.northwinds_hubspot
),
remove_special_characters AS (
    SELECT
        -- First, replace all specified special characters with spaces
        REPLACE(REPLACE(REPLACE(REPLACE(business_name, '(', ' '), ')', ' '), '.', ' '), ',', ' ') AS business_name_no_special,
        business_name 
    FROM 
        source
),
normalize_spaces AS (
    SELECT
        -- Replace multiple spaces with a single space to avoid "--" in the final output
        REGEXP_REPLACE(business_name_no_special, '\\s+', '', 'g') AS business_name_single_space,
        business_name
    FROM
        remove_special_characters
),
replace_space_with_hyphen AS (
    SELECT
        -- Now replace single spaces with hyphens
        REPLACE(business_name_single_space, ' ', '-') AS business_name_update,
        business_name
    FROM
        normalize_spaces
),
final AS (
    SELECT 
        CONCAT('hubspot-',lower(business_name_update)) AS company_id,
        business_name
    FROM replace_space_with_hyphen
)

SELECT * FROM final