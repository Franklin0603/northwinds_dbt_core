WITH source_orders AS (
    SELECT 
        order_id, 
        -- Use jinja to prepend 'rds-' to specified columns
        {% for column in ['employee_id', 'customer_id']%}
            CONCAT('rds-',{{ column }}) AS {{ column}},
        {% endfor %}
        order_date
    FROM 
        {{ source('rds', 'orders')}}
),
source_order_details AS (
    SELECT
       order_id,
       -- prepends 'rds' to product_id
       CONCAT('rds-', product_id) AS product_id,
       quantity, 
       discount, 
       unit_price 
    FROM    
        {{ source('rds', 'order_details')}}
),
group_order_product AS (
    SELECT
        orders.order_id,
        orders.employee_id,
        orders.customer_id,
        orders.order_date,
        order_details.product_id,
        order_details.quantity, 
        order_details.discount, 
        order_details.unit_price 
    FROM source_orders orders
    LEFT OUTER JOIN source_order_details order_details
        ON orders.order_id = order_details.order_id

)
SELECT * FROM group_order_product