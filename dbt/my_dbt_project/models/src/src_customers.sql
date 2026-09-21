with raw_customers as (
    select * from {{ source( 'faker', 'customers' ) }}
)

select 
    customer_id,
    name as full_name,
    TRIM(REGEXP_REPLACE( name , r'(?i)^(mr|mrs|ms|miss)\.?\s+', '')) as full_name_cleaned,
    email as customer_email,
    state as customer_state,
    signup_date as customer_signup_date

from raw_customers