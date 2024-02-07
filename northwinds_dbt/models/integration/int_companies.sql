WITH hubspot_companies AS (
    SELECT 
        company_id,
        name
    FROM 
        {{ ref('stg_hubspot_companies') }}
), 
rds_companies AS (
    SELECT 
        company_id,
        name,
        address,
        postal_code,
        city, 
        country  
    FROM 
        {{ ref('stg_rds_companies') }}
), 
merged_companies as (
    SELECT 
        company_id as hubspot_company_id, 
        null as rds_company_id, 
        name
    FROM 
        hubspot_companies 
    
    UNION ALL
    
    SELECT 
        NULL as hubspot_company_id, 
        company_id as rds_company_id, 
        name
    FROM 
        rds_companies
), 
deduped as (
    SELECT 
        max(hubspot_company_id) as hubspot_company_id,
        max(rds_company_id) as rds_company_id, 
        name
     FROM 
        merged_companies 
    GROUP BY 
        name
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['deduped.name']) }} as company_pk, 
    deduped.hubspot_company_id, 
    deduped.rds_company_id, 
    deduped.name, 
    rds_companies.address,
    rds_companies.postal_code, 
    rds_companies.city, 
    rds_companies.country 
FROM deduped
LEFT OUTER JOIN rds_companies 
    ON rds_companies.company_id = deduped.rds_company_id