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


def insert_log_entry():
    print("Starting Ingestion DAG Process")
    print("Starting Insert Log Entry Task")
    print("Inserting Log Record.")

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
        print(results.to_dict())

    # results = (
    #     monitoring_client.query(ProcessRunLog).update({"status": "completed"}).all()
    # )

    # for result in results:
    #     print("result : ", result.to_dict())

    # print("")


def update_monitoring_record(model, update_dict: dict, _id):
    monitoring_client = MonitoringClient()
    results = (
        monitoring_client.query(model).update(update_dict).where(model.id == _id).all()
    )
    return results


def ingest_data():
    print("Starting Data Ingestion task.")
    api = FlightApiClient()

    try:
        airlines = api.get_airlines()
        print(f"Got {len(airlines)} airlines.")
        flights = api.get_airline_flights(airlines[:10])
        print(f"Got {len(flights)} flights")
        details = api.get_flight_details(flights)
        print(f"Got Details for flight : {len(details)}")
    except Exception as e:
        print(traceback.print_exc())
        print(f"Failed to ingest data with : {e.__class__} : {e}")
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