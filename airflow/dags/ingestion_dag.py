from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow import DAG

from src.ingest.main import insert_log_entry,ingest_data


with DAG(
    dag_id="data_ingestion_data",
    description="DAG to ingest data from api and upload to s3",
    start_date=datetime(2025, 9, 29),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:
    process_log_insert_task = PythonOperator(task_id="Process Log Insertion Task", python_callable=insert_log_entry)
    ingest_data_task = PythonOperator(task_id = "Data Extraction Task", python_callable=ingest_data)

process_log_insert_task >> ingest_data_task
