# task to clean files after upload
# task to perform xcom cleanup


import os

from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog

from src.core.logger import console
from src.core.utils import pull_from_xcom


def remove_local_files_task(**context):
    try:
        _id = pull_from_xcom("process_log_insert_task", "inserted_id", **context)
        local_read_path = pull_from_xcom(
            "refresh_airline_task", "airlines_filepath", **context
        )

        monitoring_client = MonitoringClient()
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Running",
                "task": "remove_local_files_task_airlines",
            }
        ).all()

        if os.path.exists(local_read_path):
            os.remove(local_read_path)
            console.info(f"Removed file {local_read_path}")
        else:
            console.info(f"Given path {local_read_path} does not exists.")

        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Completed",
                "task": "remove_local_files_task_airlines",
            }
        ).all()

    except Exception as e:
        console.error(f"Failed to upload s3 : {e.__class__} : {e}")
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Failed",
                "task": "remove_local_files_task_airlines",
            }
        ).all()
