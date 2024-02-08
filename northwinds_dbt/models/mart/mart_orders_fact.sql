{{ config(materialized='table') }}

WITH int_orders AS (
    SELECT
        order_pk, 
        employee_id,
        customer_id,
        order_date,
        product_id,
        quantity, 
        discount, 
        unit_price 
    FROM 
        {{ ref('int_orders')}}
),
int_contacts AS (
    SELECT 
        contact_pk,
        hubspot_contact_id, 
        rds_contact_id
    FROM 
        {{ ref('int_contacts')}}    
),
get_the_contact_pk AS (
    SELECT 
        int_orders.order_pk, 
        int_orders.employee_id,
        int_orders.customer_id,
        int_orders.order_date,
        int_orders.product_id,
        int_orders.quantity, 
        int_orders.discount, 
        int_orders.unit_price,
        int_contacts.contact_pk
    FROM int_orders
    LEFT OUTER JOIN int_contacts 
        ON int_orders.customer_id = int_contacts.rds_contact_id 
        OR int_orders.customer_id = int_contacts.hubspot_contact_id    
)

SELECT * FROM get_the_contact_pk