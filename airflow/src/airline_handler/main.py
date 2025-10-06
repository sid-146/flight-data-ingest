import os
import traceback
from datetime import datetime

from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog

from src.core.logger import console
from src.core.flight_api import FlightApiClient
from src.core.utils import pull_from_xcom


def pull_from_xcom(task_ids, key, **context):
    # This can be moved to utils or core as it is generalized function.
    value = context["ti"].xcom_pull(task_ids=task_ids, key=key)
    return value


def insert_log_entry_task(**context):
    console.info("Starting Insert Log Entry TASK.")

    monitoring_client = MonitoringClient()
    new_p = ProcessRunLog(
        dag_name="airline_refresh_DAG",
        task="insert_log_entry",
        status="Running",
    )
    inserted_id = monitoring_client.insert(new_p)
    console.info(f"Record inserted at id: {inserted_id}")

    context["ti"].xcom_push(key="inserted_id", value=inserted_id)


def refresh_airline_task(**context):
    # Todo: Complete this function to create and store file accordingly.
    # fetch id for which record was inserted.
    # Updated process status with current task status.
    # Get Flight Details using flight client
    # Store and push to xcom the file path
    # Update process Status
    try:
        _id = pull_from_xcom("process_log_insert_task", "inserted_id", **context)

        # Creating Required objects
        monitoring_client = MonitoringClient()
        f_api = FlightApiClient()

        results = (
            monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).all()
        )

        if not isinstance(results, list):
            results = [results]

        for result in results:
            console.info(result.to_dict())

        console.info("Calling API to get airlines")
        airlines = f_api.get_airlines()
        if airlines:
            console.info(f"Got {len(airlines)} airlines to process.")
        else:
            raise ValueError("API call completed but it returned empty list.")

        console.info("Updating status")
        # Todo: this update function can be moved to client
        # Issues: How to send multiple where condition (loop column: value and append to query (How to combination))
        results = (
            monitoring_client.query(ProcessRunLog)
            .where(ProcessRunLog.id == _id)
            .update(
                {
                    "status": "Completed",
                    "task": "airline_extraction_task",
                }
            )
            .all()
        )

        console.info([result.to_dict() for result in results])

    except Exception as e:
        console.error(f"Failed to get_airlines : {e.__class__.__name__} : {e}")
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Failed",
                "errors": f"Failed to get_airlines : {e.__class__.__name__} : {e}",
                "task": "airline_extraction_task",
            }
        ).all()


def upload_s3_task(**context):
    _id = pull_from_xcom("process_log_insert_task", "inserted_id", **context)
    return _id
