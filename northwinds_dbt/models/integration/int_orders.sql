WITH stg_rds_orders AS (
    SELECT 
        order_id,
        employee_id,
        customer_id,
        order_date,
        product_id,
        quantity, 
        discount, 
        unit_price 
    FROM {{ ref('stg_rds_orders')}}
), 
generate_order_pk AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['order_id', 'product_id', 'customer_id']) }} as order_pk, 
        employee_id,
        customer_id,
        order_date,
        product_id,
        quantity, 
        discount, 
        unit_price 
    FROM stg_rds_orders
)
SELECT * FROM generate_order_pk

