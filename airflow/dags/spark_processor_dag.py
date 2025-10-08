from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow import DAG

from src.processor.main import insert_log_entry_task, get_pending_jobs_task


# todo: add another task to zip the src folder. to be shared to spark,

with DAG(
    dag_id="spark_processor_DAG",
    start_date=datetime(2025, 10, 7),
    schedule="*/45 * * * *",
    catchup=False,
):
    process_log_insert_task = PythonOperator(
        task_id="process_log_insert_task",
        python_callable=insert_log_entry_task,
    )

    pending_jobs_task = PythonOperator(
        task_id="pending_jobs_task",
        python_callable=get_pending_jobs_task,
    )


process_log_insert_task >> pending_jobs_task
