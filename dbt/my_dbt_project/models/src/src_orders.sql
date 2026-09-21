
with raw_orders as (
    select * from {{ source('faker', 'orders') }}
)

select
    order_id,
    product_id,
    quantity as order_quantity,
    customer_id,
    FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%S', order_at, 'Asia/Taipei') as order_at_tw,
    order_at,
    created_at as order_created_at

from raw_orders