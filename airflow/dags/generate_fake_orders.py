from airflow.sdk import dag, task, get_current_context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import pendulum

import os
import json, csv
from pprint import pprint
from faker import Faker
import random
import pandas as pd


GCP_CONN_ID = os.getenv('GCP_CONN_ID')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_REGION = os.getenv('GCP_REGION')
GCS_BUCKET_NAME='candy-faker-ecom-data'
BQ_DATASET_NAME = 'airflow_dbt_faker_ecom_raw'
BQ_TABLE_NAME='raw_orders'


@dag(
    tags = ['ecom_project'],
    catchup = False,
    schedule = None,
    # start_date= pendulum.datetime(2026,9,1,0,0,0 , tz='Asia/Taipei'),
    # schedule="@daily",
)
def generate_fake_orders():

    @task
    def query_products():
        hook = BigQueryHook(gcp_conn_id="google_cloud_default",use_legacy_sql=False,)
        sql =f"""
                SELECT * 
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.raw_products`
                ORDER BY RAND()
                LIMIT 10
            """
        df = hook.get_df(sql=sql)
        output = df.to_json(orient='records') 
        pprint(output)
        return output


    @task
    def query_customers():
        hook = BigQueryHook(gcp_conn_id="google_cloud_default",use_legacy_sql=False,)
        sql =f"""
                SELECT * 
                FROM `{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.raw_customers`
                ORDER BY RAND()
                LIMIT 5
            """
        df = hook.get_df(sql=sql)
        output = df.to_json(orient='records') 
        pprint(output)
        return output


    @task
    def generate_orders():
        ctx = get_current_context()
        some_rand_products = json.loads( ctx['ti'].xcom_pull(task_ids="query_products") )
        some_rand_customers = json.loads( ctx['ti'].xcom_pull(task_ids="query_customers") )
        pprint(some_rand_products)
        print("------")
        pprint(some_rand_customers)
        print("------")

        logical_date = ctx["logical_date"].in_timezone("Asia/Taipei")
        run_date = logical_date.strftime("%Y-%m-%d")
        run_datetime = logical_date.strftime("%Y-%m-%d %H:%M:%S")
        print(f"""
            run_date is {run_date}
            run_datetime is {run_datetime}
        """)

        # Faker.seed(run_date)
        fake = Faker('en_US')

        orders_data = []

        for i in range(10):
            one_rand_prod = random.choice(some_rand_products)
            one_rand_cust = random.choice(some_rand_customers)

            orders_data.append(
                {
                    'order_id': fake.uuid4(),
                    'customer_id': one_rand_cust['customer_id'],
                    'product_id': one_rand_prod['product_id'],
                    'quantity': random.randint(1,4),
                    'order_at': logical_date,
                    'created_at': logical_date,
                }
            )
        pprint(orders_data)
        return orders_data


    @task
    def save_to_tmp_folder():
        ctx = get_current_context()
        orders_data = ctx['ti'].xcom_pull(task_ids='generate_orders')
        df = pd.DataFrame(orders_data)
        df.to_csv("/tmp/orders_data.csv" , header = True , index = False)
        

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id = 'upload_to_gcs',
        src = '/tmp/orders_data.csv',
        gcp_conn_id = GCP_CONN_ID,
        bucket = GCS_BUCKET_NAME,
        dst = 'orders/date={{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%d") }}/orders_{{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%dT%H:%M:%S") }}.csv',
    )


    upload_to_bq_raw = GCSToBigQueryOperator(
        task_id = 'upload_to_bq',
        bucket = GCS_BUCKET_NAME, 
        source_objects = 'orders/date={{ logical_date.in_timezone("Asia/Taipei").strftime("%Y-%m-%d") }}/*.csv',
        destination_project_dataset_table = f'{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.raw_orders',
        source_format = 'csv',
        write_disposition = 'WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        autodetect = True,
    )


    dbt_test_raw_orders = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_raw_orders',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose = True,
        overrides={
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "test",
                        "--select", 
                        "source:faker.orders", 
                    ],
                }
            ],
        },
    )


    dbt_run_src_fct_orders = CloudRunExecuteJobOperator(
        task_id = 'dbt_run_src_fct_orders',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose = True,
        overrides={
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "run",
                        "--select", 
                        "src_orders", 
                        "fct_orders", 
                    ],
                }
            ],
        },
    )


    dbt_test_fct_orders = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_fct_orders',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose = True,
        overrides={
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "test",
                        "--select", 
                        "fct_orders", 
                    ],
                }
            ],
        },
    )


    trigger_dag_run_marts_table = TriggerDagRunOperator(
        task_id = 'trigger_dag_run_marts_table',
        trigger_dag_id = 'run_mart_tables',
        deferrable = True,
        wait_for_completion=False,
    )


    [query_products() , query_customers()] >> generate_orders() >> save_to_tmp_folder() >> upload_to_gcs >> upload_to_bq_raw >> dbt_test_raw_orders >> dbt_run_src_fct_orders >> dbt_test_fct_orders >> trigger_dag_run_marts_table

generate_fake_orders()