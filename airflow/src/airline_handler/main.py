import os

from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog, IngestionFileLog

from src.core.logger import console
from src.core.flight_api import FlightApiClient
from src.core.utils import pull_from_xcom, compress
from src.core.s3_client import S3Client
from src.core.settings import s3_bucket, aws_secret_key, aws_access_key


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


def refresh_airline_task(**context):
    try:
        _id = pull_from_xcom("process_log_insert_task", "inserted_id", **context)

        # Creating Required objects
        monitoring_client = MonitoringClient()
        f_api = FlightApiClient()

        results = (
            monitoring_client.query(ProcessRunLog)
            .where(ProcessRunLog.id == _id)
            .update(
                {
                    "status": "Running",
                    "task": "refresh_airline_task",
                }
            )
            .all()
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

        # creates folder in opt/airflow/store/ so on..
        store_path = os.path.join("store", "airlines")
        file_prefix = "airlines"

        filepath = compress(airlines, store_path, file_prefix, 7)

        context["ti"].xcom_push(key="airlines_filepath", value=filepath)
        context["ti"].xcom_push(key="total_airlines", value=len(airlines))

        console.info("Updating status")
        # Todo: this update function can be moved to client
        # Issues: How to send multiple where condition (loop column: value and append to query (How to combination))
        results = (
            monitoring_client.query(ProcessRunLog)
            .where(ProcessRunLog.id == _id)
            .update(
                {
                    "status": "Completed",
                    "task": "refresh_airline_task",
                }
            )
            .all()
        )
    except Exception as e:
        console.error(f"Failed to get_airlines : {e.__class__.__name__} : {e}")
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Failed",
                "errors": f"Failed to get_airlines : {e.__class__.__name__} : {e}",
                "task": "refresh_airline_task",
            }
        ).all()


def upload_s3_task(**context):
    try:
        _id = pull_from_xcom("process_log_insert_task", "inserted_id", **context)
        local_read_path = pull_from_xcom(
            "refresh_airline_task", "airlines_filepath", **context
        )
        no_records = pull_from_xcom("refresh_airline_task", "total_airlines", **context)
        monitoring_client = MonitoringClient()
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Running",
                "task": "upload_s3_task",
            }
        ).all()

        console.info(f"Got stored path : {local_read_path}")

        s3_client = S3Client(
            bucket=s3_bucket,
            access_key=aws_access_key,
            secret_key=aws_secret_key,
        )

        temp = local_read_path.split("/")[2:]
        s3_key = os.path.join(*temp)

        s3_path = s3_client.put_file(local_read_path, s3_key)

        console.info(f"File Uploaded at path : {s3_path}")

        monitoring_client.insert(
            IngestionFileLog(
                run_id=_id,
                s3_key=s3_path,
                no_records=no_records,
                is_processed=False,
                data_type="airlines",
            )
        )
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Completed",
                "task": "upload_s3_task",
            }
        ).all()

    except Exception as e:
        console.error(f"Failed to upload s3 : {e.__class__} : {e}")
        monitoring_client.query(ProcessRunLog).where(ProcessRunLog.id == _id).update(
            {
                "status": "Failed",
                "task": "upload_s3_task",
            }
        ).all()
