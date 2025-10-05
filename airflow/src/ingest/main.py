# import os
# import json
# from typing import List, Callable, Dict, Any, Tuple
# import gzip
# from concurrent.futures import ThreadPoolExecutor, as_completed, Future

# import requests
# from FlightRadar24.api import FlightRadar24API
# from FlightRadar24 import Flight

# from core.logger import logger
# from core.utils import compress, generate_futures, upload_s3
# from core.flight_api import FlightApiClient

import os
import traceback
from datetime import datetime

from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog

from src.core.flight_api import FlightApiClient
from src.core.s3_client import S3Client
from src.core.logger import console


def insert_log_entry():
    console.info("Starting Ingestion DAG Process")
    console.info("Starting Insert Log Entry Task")
    console.info("Inserting Log Record.")

    monitoring_client = MonitoringClient()
    new_p = ProcessRunLog(dag_name="Ingestion DAG", status="Running")
    inserted_id = monitoring_client.insert(new_p)

    # Building query using QueryBuilder Class
    results = (
        monitoring_client.query(ProcessRunLog)
        .where(ProcessRunLog.id == inserted_id)
        .all()
    )

    if not isinstance(results, list):
        results = [results]
    for results in results:
        console.info(results.to_dict())

    # results = (
    #     monitoring_client.query(ProcessRunLog).update({"status": "completed"}).all()
    # )

    # for result in results:
    #     console.info("result : ", result.to_dict())

    # console.info("")


def update_monitoring_record(model, update_dict: dict, _id):
    monitoring_client = MonitoringClient()
    results = (
        monitoring_client.query(model).update(update_dict).where(model.id == _id).all()
    )
    return results


def ingest_data():
    console.info("Starting Data Ingestion task.")
    api = FlightApiClient()

    try:
        airlines = api.get_airlines()
        console.info(f"Got {len(airlines)} airlines.")
        flights = api.get_airline_flights(airlines[:10])
        console.info(f"Got {len(flights)} flights")
        details = api.get_flight_details(flights)
        console.info(f"Got Details for flight : {len(details)}")
    except Exception as e:
        console.info(traceback.console.info_exc())
        console.info(f"Failed to ingest data with : {e.__class__} : {e}")
        update_monitoring_record(
            ProcessRunLog,
            {
                "status": "failed",
                "task": "ingestion_data",
                "error": f"Failed to ingest data with : {e.__class__} : {e}",
            },
        )


def upload_s3():
    details = []
    s3_client = S3Client(bucket="", access_key="", secret_key="", region="ap-south-1")
    bucket = os.getenv("S3_BUCKET")
    folder_name = f"flights_data_{datetime.now().strftime('%y_%m_%d_%H_%M_%S')}"

    s3_client.put_compressed_object(
        data=details, bucket_name=bucket, put_path=folder_name
    )