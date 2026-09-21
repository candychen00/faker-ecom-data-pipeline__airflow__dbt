
with raw_products as (
    select * from {{ source('faker', 'products') }}
)

select 
    product_id,
    price as product_price,
    category as product_category,
    fruit as product_fruit

from raw_products