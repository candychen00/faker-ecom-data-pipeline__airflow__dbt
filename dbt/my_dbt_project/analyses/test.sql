select *
from `project-97ea2755-962c-466f-b6c`.`airflow_dbt_faker_ecom_raw`.`raw_customers`
where ARRAY_LENGTH(SPLIT( name , ' ')) != 2