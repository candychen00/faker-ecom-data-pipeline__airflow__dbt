from airflow.sdk import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

import os
from pprint import pprint
import csv
import random


GCP_CONN_ID = os.getenv('GCP_CONN_ID')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_REGION = os.getenv('GCP_REGION')
GCS_BUCKET_NAME ='candy-faker-ecom-data'
BQ_DATASET_NAME = 'airflow_dbt_faker_ecom_raw'
BQ_TABLE_NAME ='raw_products'


@dag(
    schedule=None,
    tags=['ecom_project']
)
def generate_fake_products():

    @task
    def fake_products():
        product_fruits = ['Apple', 'Banana', 'Orange', 'Strawberry', 'Mango', 'Blueberry', 'Peach', 'Pineapple']
        product_categories = ['Juice', 'Jam', 'Dried Snacks', 'Smoothie', 'Gummies']

        product_id = -1
        all_products = []
        for fruit in product_fruits:
            for category in product_categories:
                product_id+=1
                all_products.append(
                    {
                        'product_id': product_id,
                        'fruit': fruit,
                        'category': category,
                        'price': round(random.uniform(10,20),2),
                    }
                )
        pprint(all_products)
        return all_products


    @task
    def save_to_tmp_folder(data):
        headers = data[0].keys()
        with open("/tmp/all_products.csv", mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames = headers)
            writer.writeheader()
            writer.writerows(data)


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        gcp_conn_id= GCP_CONN_ID,
        bucket= GCS_BUCKET_NAME,
        src="/tmp/all_products.csv",
        dst="products/all_products.csv",
    )


    upload_to_bq = GCSToBigQueryOperator(
        task_id = 'upload_to_bq',
        gcp_conn_id= GCP_CONN_ID,
        bucket = GCS_BUCKET_NAME,
        source_objects = "products/all_products.csv",
        source_format = 'csv',
        destination_project_dataset_table= f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        # autodetect =True,
        schema_fields = [
            {"name": "fruit", "type": "STRING", "mode": "REQUIRED"},
            {"name": "price", "type": "NUMERIC", "mode": "REQUIRED"},
            {"name": "category", "type": "STRING", "mode": "REQUIRED"},
            {"name": "product_id", "type": "INT64", "mode": "REQUIRED"},
        ],
        write_disposition="WRITE_TRUNCATE",       # 或 WRITE_APPEND / WRITE_EMPTY
        create_disposition="CREATE_IF_NEEDED",    # 表不存在就建立
    )


    dbt_test_raw_products = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_raw_products',
        gcp_conn_id = GCP_CONN_ID, 
        project_id = GCP_PROJECT_ID, 
        region = GCP_REGION,
        job_name = 'mydbt2', 
        timeout_seconds = 60*2,
        deferrable = True,
        verbose = True,
        overrides = {
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "test",
                        "--select", 
                        "source:faker.customers",
                    ],
                }
            ],
        }
    )


    dbt_run_src_products = CloudRunExecuteJobOperator(
        task_id = 'dbt_run_src_products',
        gcp_conn_id = GCP_CONN_ID, 
        project_id = GCP_PROJECT_ID, 
        region = GCP_REGION,
        job_name = 'mydbt2', 
        timeout_seconds = 60*2,
        deferrable = True,
        verbose = True,
        overrides = {
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "run",
                        "--select", 
                        "src_customers",
                    ],
                }
            ],
        }
    )


    dbt_run_dim_products = CloudRunExecuteJobOperator(
        task_id = 'dbt_run_dim_products',
        gcp_conn_id = GCP_CONN_ID, 
        project_id = GCP_PROJECT_ID, 
        region = GCP_REGION,
        job_name = 'mydbt2', 
        timeout_seconds = 60*2,
        deferrable = True,
        verbose = True,
        overrides = {
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "run",
                        "--select", 
                        "dim_customers",
                    ],
                }
            ],
        }
    )


    dbt_test_dim_products = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_dim_products',
        gcp_conn_id = GCP_CONN_ID, 
        project_id = GCP_PROJECT_ID, 
        region = GCP_REGION,
        job_name = 'mydbt2', 
        timeout_seconds = 60*2,
        deferrable = True,
        verbose = True,
        overrides = {
            "container_overrides": [
                {
                    "args": [
                        "dbt", 
                        "test",
                        "--select", 
                        "dim_customers",
                    ],
                }
            ],
        }
    )


    data = fake_products()
    save_to_tmp_folder( data ) >> upload_to_gcs >> upload_to_bq >> dbt_test_raw_products >> dbt_run_src_products >> dbt_run_dim_products >> dbt_test_dim_products


generate_fake_products()