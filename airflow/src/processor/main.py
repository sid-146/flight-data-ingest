"""
Todo:
    decide the flow; How to identify which job to trigger using File Ingestion Table;

    # First Think of only how to process airlines (Think for multiple files)

"""

import os
import json
from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog, IngestionFileLog

from src.core.logger import console
from src.core.utils import pull_from_xcom


# Todo: Better logging
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


def get_pending_jobs_task(**context):
    _id = pull_from_xcom(
        task_ids="process_log_insert_task", key="inserted_id", **context
    )
    monitoring_client = MonitoringClient()
    monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
        {"task": "get_pending_jobs_task"}
    )

    records = (
        monitoring_client.query(IngestionFileLog)
        .where(IngestionFileLog.is_processed == "pending")
        .all()
    )

    if not isinstance(records, list):
        records = [records]

    records = [record.to_dict() for record in records]
    for record in records:
        console.info(record)

    context["ti"].xcom_push(key="get_pending_jobs_task", value=json.dumps(records))

    monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
        {"status": "Completed"}
    )

    return records
