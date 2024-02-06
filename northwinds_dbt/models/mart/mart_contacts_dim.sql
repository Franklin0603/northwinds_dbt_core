{{ config(materialized='table') }}


WITH int_companies AS (
    SELECT 
        company_pk,
        hubspot_company_id,
        rds_company_id
    FROM 
        {{ ref('int_companies') }}
),
int_contacts AS (
    SELECT 
        contact_pk,
        hubspot_company_id,
        rds_company_id,
        first_name, 
        last_name, 
        phone
    FROM {{ ref('int_contacts') }}
),
add_company_pk_to_contacts AS (
    SELECT 
        int_contacts.contact_pk, 
        int_contacts.first_name, 
        int_contacts.last_name, 
        int_contacts.phone,
        int_companies.company_pk
    FROM int_contacts
    LEFT OUTER JOIN int_companies 
        ON  int_contacts.hubspot_company_id = int_companies.hubspot_company_id 
        OR  int_contacts.rds_company_id     = int_companies.rds_company_id
)

SELECT 
    contact_pk, 
    first_name, 
    last_name, 
    phone,
    company_pk
FROM 
    add_company_pk_to_contacts 