{{ config(materialized='table') }}


WITH int_companies as (
    SELECT 
        company_pk, 
        name, 
        address, 
        postal_code, 
        city, 
        country  
    FROM 
        {{ ref('int_companies') }}
)
SELECT 
    company_pk, 
    name, 
    address, 
    postal_code, 
    city, 
    country 
FROM 
    int_companies 