import os
import json
from typing import List, Callable
import gzip
from concurrent.futures import ThreadPoolExecutor, as_completed

from FlightRadar24.api import FlightRadar24API
from FlightRadar24 import Flight

from core.logger import logger
from core.utils import compress


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


def try_retry_with_other_option(url):
    return


def generate_futures(func:Callable,**kwargs):
    return


def make_api_call(api: FlightRadar24API, flights: List[Flight]):
    max_workers = min(len(flights), 4)
    results: List[dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(get_flight_details, api, flight): flight for flight in flights
        }

    for future in as_completed(futures):
        try:
            args = futures[future]
            result = future.result()
            results.append(result)
        except Exception as e:
            # retry using url given in the error message.
            print(f"Failed to get detail")


def main():
    api = FlightRadar24API()
    flights = get_airlines_current_flight(api)

    results = make_api_call(api, flights)

    return
