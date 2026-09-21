{#
    config(
        materialized='incremental',
        incremental_strategy= 'merge',
        unique_key= 'order_id',
    )
#}

with src_orders as (
    select * from {{ ref('src_orders') }}
)

select
    ord.*,
    ord.order_quantity * prod.product_price as order_amount,

    product_name,
    product_price,
    product_fruit,
    product_category,

    full_name_cleaned,
    first_name,
    last_name,
    customer_email,
    customer_state,
    customer_signup_date


from src_orders ord
inner join {{ ref('dim_products') }} prod on ord.product_id = prod.product_id
inner join {{ ref('dim_customers') }} cust on ord.customer_id = cust.customer_id


{% if is_incremental() %}

    WHERE order_created_at >= ( select max(order_created_at) from {{this}} )

{% endif %}