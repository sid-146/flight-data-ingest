import os
import traceback
from datetime import datetime

from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog

from src.core.logger import console
from src.core.flight_api import FlightApiClient


def insert_log_entry(**context):
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


def get_airlines(**context):
    # Todo: Complete this function to create and store file accordingly.
    try:
        inserted_id = context["ti"].xcom_pull(
            task_ids="process_log_insert_task", key="inserted_id"
        )

        monitoring_client = MonitoringClient()
        results = (
            monitoring_client.query(ProcessRunLog)
            .where(ProcessRunLog.id == inserted_id)
            .all()
        )

        if not isinstance(results, list):
            results = [results]

        for result in results:
            console.info(result.to_dict())

        console.info("Updating status")
        results = (
            monitoring_client.query(ProcessRunLog)
            .where(ProcessRunLog.id == inserted_id)
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
        monitoring_client.query(ProcessRunLog).where(
            ProcessRunLog.id == inserted_id
        ).update(
            {
                "status": "Failed",
                "errors": f"Failed to get_airlines : {e.__class__.__name__} : {e}",
                "task": "airline_extraction_task",
            }
        ).all()
