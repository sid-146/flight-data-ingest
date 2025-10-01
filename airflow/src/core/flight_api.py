# Sort import properly
import traceback

import requests
from typing import List
from concurrent.futures import as_completed

from FlightRadar24.api import FlightRadar24API, Flight

from src.core.utils import generate_futures


class FlightApiClient:
    def __init__(self):
        self.api = FlightRadar24API()

    def get_airlines(self):
        airlines = self.api.get_airlines()
        return airlines

    @staticmethod
    def _flight_details(api: FlightRadar24API, flight):
        flight = api.get_flight_details(flight)
        return flight

    @staticmethod
    def get_airline_current_flights(api: FlightRadar24API, airline):
        flights = api.get_flights(airline["ICAO"])
        return flights

    def _retry_with_other_option(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_airlines_current_flights(self, airlines: List[dict]) -> List[Flight]:
        futures = generate_futures(
            self.get_airline_current_flights,
            [(self.api, airline) for airline in airlines],
        )

        flights: List[dict] = []
        for future in as_completed(futures):
            try:
                args = futures[future]
                _flights = future.result()
                flights.extend(_flights)
            except Exception as e:
                traceback.print_exc()
                print(f"Failed to get flight detail for : {args} : {e}")

        print(f"flights : {flights}")
        return flights

    def get_flights_details(self, flights: List[Flight]):
        print(flights[0])
        futures = generate_futures(
            self._flight_details, [(self.api, flight) for flight in flights]
        )

        details: List[dict] = []
        for future in as_completed(futures):
            try:
                args = futures[future]
                _detail = future.result()
                details.append(_detail)
            except requests.exceptions.HTTPError as e:
                url = e.request.url
                _details = self._retry_with_other_option(url)
                details.append(_details)
            except Exception as e:
                print(f"Failed to get detail for : {args} : {e}")

        return details
