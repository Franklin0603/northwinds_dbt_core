with source as (
    SELECT * FROM public.customers
), 
renamed as (
    SELECT
        customer_id,
        country,
        split_part(contact_name, ' ', 1) as first_name,
        split_part(contact_name, ' ', 2) as last_name
    FROM source 
)

SELECT * FROM renamed