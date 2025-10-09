from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# --- Basic DAG setup ---
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="dynamic_spark_trigger_dag",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,  # run manually for now
    catchup=False,
    max_active_tasks=2,  # limit 2 concurrent dynamic tasks
    concurrency=2,  # DAG-level concurrency limit
    tags=["dynamic", "spark"],
) as dag:

    # Step 1: Simulate reading from metadata table
    @task
    def extract_metadata():
        """
        Simulate querying metadata table and returning pending jobs.
        Replace this later with actual DB query.
        """
        metadata_records = [
            {"id": 1, "job_name": "load_users", "file_path": "/bronze/users.csv"},
            {"id": 2, "job_name": "load_payments", "file_path": "/bronze/payments.csv"},
            {"id": 3, "job_name": "load_orders", "file_path": "/bronze/orders.csv"},
        ]
        return metadata_records

    # Step 2: Dynamic task - process each metadata record
    @task
    def process_job(metadata_record):
        """
        Placeholder for Spark job trigger.
        Currently just prints which job would be triggered.
        """
        job_name = metadata_record["job_name"]
        file_path = metadata_record["file_path"]
        print(f"Triggering Spark job: {job_name} for file {file_path}")
        # TODO: Replace this print with actual SparkSubmitOperator or REST trigger
        return f"Completed {job_name}"

    # Step 3: Map metadata to dynamic tasks
    records = extract_metadata()
    process_job.expand(metadata_record=records)
