
with src_customers as (

    select * from {{ ref('src_customers') }}

)

select 
    customer_id,
    full_name,
    full_name_cleaned,
    SPLIT( full_name_cleaned , ' ')[OFFSET(0)] AS first_name,
    SPLIT( full_name_cleaned , ' ')[OFFSET(1)] AS last_name,
    customer_email,
    customer_state,
    customer_signup_date

from src_customers