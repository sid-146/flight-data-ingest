from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow import DAG

from src.airline_handler.main import (
    insert_log_entry_task,
    refresh_airline_task,
    upload_s3_task,
)


with DAG(
    dag_id="airline_refresh_DAG",
    description="DAG to ingest data from api and upload to s3",
    start_date=datetime(2025, 10, 5),
    schedule="*/45 * * * *",
    catchup=False,
) as dag:
    process_log_insert_task = PythonOperator(
        task_id="process_log_insert_task", python_callable=insert_log_entry_task
    )
    airlines_extraction_task = PythonOperator(
        task_id="refresh_airline_task", python_callable=refresh_airline_task
    )
    s3_upload_task = PythonOperator(
        task_id="s3_upload_task", python_callable=upload_s3_task
    )


process_log_insert_task >> airlines_extraction_task >> s3_upload_task
