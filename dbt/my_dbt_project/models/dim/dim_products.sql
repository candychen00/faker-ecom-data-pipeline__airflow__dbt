
WITH src_products AS (

    select * from {{ ref('src_products') }}

)

SELECT
    product_id,
    CONCAT( product_fruit, " ", product_category) as product_name,
    product_price,
    product_fruit,
    product_category
    

FROM src_products
ORDER BY product_id