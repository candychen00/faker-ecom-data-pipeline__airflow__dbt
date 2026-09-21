
{{
    config(
        schema='airflow_dbt_faker_ecom_mart',
    )
}}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
)

select 
    SUBSTR(order_at_tw, 1, 10) as order_date,
    product_category,
    product_fruit,
    sum(order_quantity) as units_sold,
    count(*) as orders_made,
    sum(order_amount) as sales_amount

from fct_orders
where DATE(PARSE_DATETIME('%Y-%m-%d %H:%M:%S', order_at_tw)) >= DATE_SUB(CURRENT_DATE('Asia/Taipei'), INTERVAL 15 DAY)
group by order_date, product_category, product_fruit
order by order_date desc , product_category, product_fruit