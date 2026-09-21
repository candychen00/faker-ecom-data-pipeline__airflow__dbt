
{{
    config(
        schema='airflow_dbt_faker_ecom_mart',
    )
}}

with fct_orders as (
    select * from {{ ref('fct_orders') }}
)

select 
    SUBSTR(order_at_tw, 1, 7) as year_months,
    product_category,
    product_fruit,
    sum(order_quantity) as units_sold,
    count(*) as orders_made,
    sum(order_amount) as sales_amount

from fct_orders
group by year_months, product_category, product_fruit
order by year_months desc , product_category, product_fruit