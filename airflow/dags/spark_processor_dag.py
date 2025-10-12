import json
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

from src.processor.main import insert_log_entry_task, get_pending_jobs_task
from src.core.logger import console
from src.core.utils import pull_from_xcom


# todo: add another task to zip the src folder. to be shared to spark,

job_file_path_map = {
    "airlines": "/opt/airflow/src/processor/jobs/airlines_processor.py"
}

SPARK_PACKAGES = [
    "org.postgresql:postgresql:42.5.0",  # JDBC driver for PostgreSQL
    "org.apache.hadoop:hadoop-aws:3.3.2",  # S3 connector
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",  # AWS SDK bundle (dependency for hadoop-aws)
]


# DAG definition
with DAG(
    dag_id="spark_processor_S3_to_PG_DAG",
    start_date=datetime(2025, 10, 7),
    schedule="*/45 * * * *",  # runs every 45 minutes
    catchup=False,
    # max_active_tasks=2,  # Limit 2 spark jobs at a time
    # concurrency=2,
    tags=["spark", "dynamic"],
) as dag:

    # Step 1: Insert process log entry
    process_log_insert_task = PythonOperator(
        task_id="process_log_insert_task",
        python_callable=insert_log_entry_task,
    )

    # Step 2: Get pending jobs metadata from DB (returns list of dicts)
    pending_jobs_task = PythonOperator(
        task_id="pending_jobs_task",
        python_callable=get_pending_jobs_task,
    )

    # Step 3: Define SparkSubmitOperator with partial args
    spark_job_template = SparkSubmitOperator.partial(
        task_id="spark_job_template",
        conn_id="spark_default",
        packages=",".join(SPARK_PACKAGES),
    ).expand(
        application=pending_jobs_task.output.map(
            lambda record: job_file_path_map.get(record["data_type"])
        ),
        application_args=pending_jobs_task.output.map(
            lambda record: [
                f"--s3_uri={record['s3_key']}",
                f"--job_name={record['data_type']}",
                f"--run_id={record['run_id']}",
                f"--file_log_id={record['id']}",
            ]
        ),
    )

    # Todo: add zip task
    process_log_insert_task >> pending_jobs_task >> spark_job_template
