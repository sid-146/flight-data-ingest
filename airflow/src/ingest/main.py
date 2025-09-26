import os
import json
from typing import List, Callable, Dict, Any, Tuple
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed, Future

from FlightRadar24.api import FlightRadar24API
from FlightRadar24 import Flight

from core.logger import logger
from core.utils import compress, generate_futures


def get_flight_details(api: FlightRadar24API, flight: Flight):
    result = api.get_flight_details(flight)
    return result


def get_airlines_current_flight(api: FlightRadar24API):
    airlines = api.get_airlines()
    flights: List[Flight] = []

    for airline in airlines:
        _flight = api.get_flights(airline)
        flights.extend(_flight)

    print(f"Found total flights : {len(flights)} for {len(airlines)} airlines.")

    return flights


def retry_with_other_option(url: str) -> dict:
    return


# def generate_futures(
#     func: Callable, *args: List[Tuple[Any, ...]], max_workers: int = 4
# ) -> Dict[Future, Tuple[Any, ...]]:
#     # generic function to create futures, returns dictionary {futures:args}
#     futures: Dict[Future, Tuple[Any, ...]] = {}
#     with ThreadPoolExecutor(max_workers=max_workers) as pool:
#         for arg in args:
#             future = pool.submit(func, *arg)
#             futures[future] = arg

#     return futures


def make_api_call(api: FlightRadar24API, flights: List[Flight]):
    max_workers = min(len(flights), 4)
    results: List[dict] = []

    args_list: List[Tuple[Any, ...]] = [(api, flight) for flight in flights]
    futures = generate_futures(get_flight_details, args_list, max_workers=max_workers)

    for future in as_completed(futures):
        try:
            args = futures[future]
            result = future.result()
            results.append(result)
        except Exception as e:
            # retry using url given in the error message.
            result = retry_with_other_option("")
            if not result:
                print(f"Retry also failed for {args}")
            else:
                results.append(result)
            print(f"Failed to get detail for : {args} : {e}")

    return results


def main():
    api = FlightRadar24API()
    flights = get_airlines_current_flight(api)
    results = make_api_call(api, flights)

    return
