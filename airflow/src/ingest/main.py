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

from src.db.monitoring_db_client import MonitoringClient
from src.db.models.monitoring_models import ProcessRunLog
from src.db.BaseDBClient import QueryBuilder


def main():
    print("Starting Ingestion DAG Process")
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

    for results in results:
        print(results.to_dict())

    results = (
        monitoring_client.query(ProcessRunLog).update({"status": "completed"}).all()
    )

    for result in results:
        print("result : ", result.to_dict())


# def get_flight_details(api: FlightRadar24API, flight: Flight):
#     result = api.get_flight_details(flight)
#     return result


# def get_airlines_current_flight(api: FlightRadar24API):
#     airlines = api.get_airlines()
#     flights: List[Flight] = []

#     for airline in airlines:
#         _flight = api.get_flights(airline)
#         flights.extend(_flight)

#     print(f"Found total flights : {len(flights)} for {len(airlines)} airlines.")

#     return flights


# def retry_with_other_option(url: str) -> dict:
#     response = requests.get(url)
#     response.raise_for_status()
#     return response.json()


# def make_api_call(api: FlightRadar24API, flights: List[Flight]):
#     max_workers = min(len(flights), 4)
#     results: List[dict] = []

#     args_list: List[Tuple[Any, ...]] = [(api, flight) for flight in flights]
#     futures = generate_futures(get_flight_details, args_list, max_workers=max_workers)

#     for future in as_completed(futures):
#         try:
#             args = futures[future]
#             result = future.result()
#             results.append(result)
#         except requests.exceptions.HTTPError as e:
#             url = e.request.url
#             result = retry_with_other_option(url=url)
#         except Exception as e:
#             print(f"Failed to get detail for : {args} : {e}")

#     return results


# def main():
#     api = FlightRadar24API()
#     flights = get_airlines_current_flight(api)
#     results = make_api_call(api, flights)
#     store_path = os.path.join("store")
#     path = compress(results, store_path, ratio=8)
#     upload_s3(read_path=path)
#     return
