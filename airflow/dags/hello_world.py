from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


# 1. Define the Python function that will be executed
def hello_world():
    print("Hello World")


# 2. Define the DAG
with DAG(
    dag_id="hello_world_dag",  # Unique DAG ID
    description="A simple Hello World DAG",
    start_date=datetime(2025, 9, 21),  # DAG start date
    # schedule_interval="@daily",  # Run once every day
    schedule_interval="*/1 * * * *",  # Run once every day
    catchup=False,  # Don't run past dates
) as dag:

    # 3. Define the task using PythonOperator
    hello_task = PythonOperator(task_id="say_hello", python_callable=hello_world)

# 4. Set task dependencies (optional here, only one task)
hello_task
