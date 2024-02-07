{% set sources = ["stg_hubspot_companies", "stg_rds_companies"] %}

-- Merge companies from different sources into a single dataset
WITH merged_companies AS (
    {% for source in sources %}
    -- Select company data from each source, dynamically assign IDs based on the source
    SELECT 
        name, 
        -- Use Jinja templating to conditionally assign hubspot_company_id
        {{ 'company_id' if 'hubspot' in source else 'NULL' }} as hubspot_company_id,
        -- Use Jinja templating to conditionally assign rds_company_id
        {{ 'company_id' if 'rds' in source else 'NULL' }} as rds_company_id
    FROM {{ ref(source) }}
    -- Use UNION ALL to combine data from all sources
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
),
deduped AS (
    -- Deduplicate companies by name, aggregating company IDs
    SELECT 
        MAX(hubspot_company_id) as hubspot_company_id, 
        MAX(rds_company_id) as rds_company_id, 
        name 
    FROM merged_companies 
    GROUP BY name
)
-- Final selection of company information, including a generated surrogate key
SELECT 
    -- Generate a unique key for each company based on its name
    {{ dbt_utils.generate_surrogate_key(['deduped.name']) }} as company_pk, 
    deduped.hubspot_company_id, 
    deduped.rds_company_id, 
    deduped.name, 
    rds_companies.address,
    rds_companies.postal_code, 
    rds_companies.city, 
    rds_companies.country 
FROM deduped
-- Enrich dataset by joining with RDS companies for additional details
JOIN {{ ref('stg_rds_companies') }} rds_companies 
    ON rds_companies.company_id = deduped.rds_company_id
