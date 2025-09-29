from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow import DAG

from src.ingest.main import main


with DAG(
    dag_id="data_ingestion_data",
    description="DAG to ingest data from api and upload to s3",
    start_date=datetime(2025, 9, 29),
    schedule="*/5 * * * *",
    catchup=False,
) as dag:
    run_task = PythonOperator(task_id="ingestion", python_callable=main)

run_task
