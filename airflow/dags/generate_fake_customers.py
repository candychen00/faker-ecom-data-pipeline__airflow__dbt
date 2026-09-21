from airflow.sdk import dag, task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator

from faker import Faker
from datetime import date
from pprint import pprint

import os
from pprint import pprint
import csv


GCP_CONN_ID = os.getenv('GCP_CONN_ID')
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCP_REGION = os.getenv('GCP_REGION')
GCS_BUCKET_NAME='candy-faker-ecom-data'
BQ_DATASET_NAME = 'airflow_dbt_faker_ecom_raw'
BQ_TABLE_NAME='raw_customers'


@dag(
    schedule=None,
    tags=['ecom_project']
)
def generate_fake_customers():


    @task
    def fake_customers():
        Faker.seed(1234)
        fake = Faker('en_US')

        fake_customers=[]
        for i in range(20):
            fake_customers.append(
                {
                    'customer_id': fake.uuid4(),
                    'name': fake.name(),
                    'email': fake.email(),
                    'state': fake.state(),
                    'signup_date': fake.date_between(start_date= date(2026, 1, 1), end_date=date(2026, 6, 30)),
                }
            )
        pprint(fake_customers)
        return fake_customers


    @task
    def save_to_tmp_folder(data):
        headers = data[0].keys()
        with open("/tmp/all_customers.csv", mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames = headers)
            writer.writeheader()
            writer.writerows(data)


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "upload_to_gcs",
        gcp_conn_id = GCP_CONN_ID,
        bucket = GCS_BUCKET_NAME,
        src = "/tmp/all_customers.csv",
        dst = "customers/all_customers.csv",
    )


    upload_to_bq = GCSToBigQueryOperator(
        task_id = 'upload_to_bq',
        gcp_conn_id = GCP_CONN_ID,
        bucket = GCS_BUCKET_NAME,
        source_objects = "customers/all_customers.csv",
        source_format = 'csv',
        destination_project_dataset_table = f"{GCP_PROJECT_ID}.{BQ_DATASET_NAME}.{BQ_TABLE_NAME}",
        # autodetect =True,
        schema_fields = [
            {"name": "name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "REQUIRED"},
            {"name": "state", "type": "STRING", "mode": "REQUIRED"},
            {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "signup_date", "type": "DATE", "mode": "REQUIRED"},
            
        ],
        write_disposition = "WRITE_TRUNCATE",       # 或 WRITE_APPEND / WRITE_EMPTY
        create_disposition = "CREATE_IF_NEEDED",    # 表不存在就建立
    )


    dbt_test_raw_customers = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_raw_customers',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose=True,
        overrides={
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
        },
    )


    dbt_run_src_customers = CloudRunExecuteJobOperator(
        task_id = 'dbt_run_src_customers',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose=True,
        overrides={
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
        },
    )


    dbt_run_dim_customers = CloudRunExecuteJobOperator(
        task_id = 'dbt_run_dim_customers',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose=True,
        overrides={
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
        },
    )


    dbt_test_dim_customers = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_dim_customers',
        gcp_conn_id = GCP_CONN_ID,
        project_id = GCP_PROJECT_ID,
        region = GCP_REGION,
        job_name = 'mydbt2',
        timeout_seconds = 60*2,
        deferrable= True,
        verbose=True,
        overrides={
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
        },
    )


    data = fake_customers()
    save_to_tmp_folder( data ) >> upload_to_gcs >> upload_to_bq >> dbt_test_raw_customers >> dbt_run_src_customers >> dbt_run_dim_customers >> dbt_test_dim_customers
  

generate_fake_customers()