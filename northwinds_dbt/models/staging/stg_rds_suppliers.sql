-- Common Table Expression (CTE) for the source data
WITH source AS (
    -- Selecting all columns from the suppliers table
    SELECT * FROM public.suppliers
),
-- Rename and format specific columns for clarity and further processing
renamed AS (
    SELECT
        supplier_id, 
        company_name,
        -- Splitting the contact_name into first and last names
        SPLIT_PART(contact_name, ' ', 1) AS contact_first_name,
        SPLIT_PART(contact_name, ' ', -1) AS contact_last_name,
        contact_title, 
        -- Removing special characters from the phone number and standardizing its format
        REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') AS phone,
        address, 
        city,
        postal_code,
        region,
        fax,
        homepage 
    FROM source
),
-- Final transformation for presenting the data
final AS (
    SELECT
        supplier_id, 
        company_name, 
        contact_first_name, 
        contact_last_name,
        contact_title,
        -- Formatting the phone number to a more readable format if it's 10 digits long
        CASE 
            WHEN LENGTH(phone) = 10 THEN '(' || SUBSTRING(phone, 1, 3) || ') ' || 
                                     SUBSTRING(phone, 4, 3) || '-' ||
                                     SUBSTRING(phone, 7, 4) 
        END AS phone,
        address, 
        city, 
        postal_code,
        region, 
        fax, 
        homepage
    FROM renamed 
)

-- Final selection from the transformed data
SELECT 
    supplier_id, 
    company_name, 
    contact_first_name, 
    contact_last_name,
    contact_title, 
    phone, 
    address, 
    city, 
    postal_code,
    region, 
    fax, 
    homepage
FROM final;
