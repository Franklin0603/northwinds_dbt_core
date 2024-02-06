WITH stg_contacts AS (
    SELECT 
        contact_id,
        first_name, 
        last_name,
        phone, 
        company_id
    FROM
        {{ ref('stg_hubspot_contacts')}}
), 
stg_customers AS (
    SELECT 
        customer_id,
        first_name, 
        last_name,
        phone, 
        company_id
    FROM 
        {{ ref('stg_rds_customers')}}
),
merged_contacts  AS (
    SELECT 
        contact_id as hubspot_contact_id,
        null as rds_contact_id,
        first_name, 
        last_name,
        phone, 
        company_id as hubspot_company_id,
        null as rds_company_id
    FROM stg_contacts
    
    UNION ALL

    SELECT 
        null as hubspot_contact_id,
        customer_id as rds_contact_id,
        first_name, 
        last_name,
        phone, 
        null as hubspot_company_id,
        company_id as rds_company_id
    FROM stg_customers 
), 
final as (
    SELECT 
        max(hubspot_contact_id) as hubspot_contact_id, 
        max(rds_contact_id) as rds_contact_id,
        first_name, 
        last_name, 
        max(phone) as phone, 
        max(hubspot_company_id) as hubspot_company_id, 
        max(rds_company_id) rds_company_id
    FROM 
        merged_contacts
    GROUP BY 
        first_name, 
        last_name
 )
 SELECT 
    {{ dbt_utils.generate_surrogate_key(['first_name', 'last_name', 'phone']) }} as contact_pk, 
    hubspot_contact_id, 
    rds_contact_id,
    first_name, 
    last_name, 
    phone, 
    hubspot_company_id, 
    rds_company_id 
FROM final 