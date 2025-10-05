from airflow import DAG
from airflow.models import XCom
from airflow.utils.session import provide_session
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator


@provide_session
def clean_old_xcoms(session=None, days_to_keep=7):
    """Delete XComs older than N days."""
    cutoff_date = datetime.now() - timedelta(days=days_to_keep)

    deleted = session.query(XCom).filter(XCom.execution_date < cutoff_date).delete()
    session.commit()

    print(f"✅ Deleted {deleted} XCom records older than {days_to_keep} days.")


with DAG(
    "house_keeping_DAG",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",  # or None if triggered manually
    catchup=False,
) as dag:
    cleanup_task = PythonOperator(
        task_id="cleanup_xcoms",
        python_callable=clean_old_xcoms,
    )
