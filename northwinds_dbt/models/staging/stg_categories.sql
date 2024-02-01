WITH transformed_categories as (
  SELECT
    category_id, 
    lower(category_name), 
    description 
  FROM categories
)

SELECT * 
FROM transformed_categories