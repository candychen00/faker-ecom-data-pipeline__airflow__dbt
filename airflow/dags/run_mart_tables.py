from airflow.sdk import dag, task, get_current_context
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
import pendulum

import os


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


def run_mart_tables():


    dbt_run_mart_tables = CloudRunExecuteJobOperator(
        task_id = 'dbt_run_mart_tables',
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
                        "-select", 
                        "path:models/mart", 
                    ],
                }
            ],
        },
    )


    dbt_test_mart_tables = CloudRunExecuteJobOperator(
        task_id = 'dbt_test_mart_tables',
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
                        "-select", 
                        "path:models/mart", 
                    ],
                }
            ],
        },
    )


    dbt_run_mart_tables >> dbt_test_mart_tables


run_mart_tables()