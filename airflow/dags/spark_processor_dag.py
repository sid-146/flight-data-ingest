from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow import DAG

from src.processor.main import insert_log_entry_task, get_pending_jobs_task
from src.core.logger import console
from src.core.utils import pull_from_xcom


# todo: add another task to zip the src folder. to be shared to spark,

# DAG definition
with DAG(
    dag_id="spark_processor_DAG",
    start_date=datetime(2025, 10, 7),
    schedule="*/45 * * * *",  # runs every 45 minutes
    catchup=False,
    max_active_tasks=2,  # Limit 2 spark jobs at a time
    concurrency=2,
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

    # Step 3: Define a function that creates Spark tasks dynamically
    def create_spark_task(metadata_record):
        job_name = metadata_record["data_type"]
        s3_key = metadata_record["s3_key"]

        return SparkSubmitOperator(
            task_id=f"spark_{job_name}",
            application="",  # Path to your Spark script
            py_files="",  # Zipped Dependencies
            conn_id="spark_default",
            conf={
                "spark.master": "spark://spark-master:7077",  # Spark master URL
                "spark.submit.deployMode": "cluster",  # Run in cluster mode
            },
            application_args=[f"--file_path={s3_key}", f"--job_name={job_name}"],
        )

    # Step 4: Python function to create tasks after reading from XCom
    def trigger_dynamic_spark_jobs(**context):
        """
        Read list of pending jobs from XCom pushed by get_pending_jobs_task,
        and dynamically create SparkSubmitOperator tasks.
        """
        pending_jobs = pull_from_xcom(
            task_ids="pending_jobs_task",
            key="get_pending_jobs_task",
            **context,
        )

        if not pending_jobs:
            console.info("✅ No pending Spark jobs found.")
            return

        for record in pending_jobs:
            spark_task = create_spark_task(record)
            pending_jobs_task >> spark_task

    # Step 5: Create a dynamic task generator
    dynamic_task_creator = PythonOperator(
        task_id="dynamic_task_creator",
        python_callable=trigger_dynamic_spark_jobs,
    )

    # DAG Dependencies
    # Todo: add zip task
    process_log_insert_task >> pending_jobs_task >> dynamic_task_creator
