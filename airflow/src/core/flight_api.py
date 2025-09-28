import requests
from typing import List
from concurrent.futures import as_completed

from FlightRadar24.api import FlightRadar24API

from core.utils import generate_futures


class FlightApiClient:
    def __init__(self):
        self.api = FlightRadar24API()

    def get_airlines(self):
        airlines = self.api.get_airlines()
        return airlines

    def get_flight_details(self, flight):
        flight = self.api.get_flight_details(flight)
        return flight

    def get_airline_current_flights(self, airline):
        flights = self.api.get_flights(airline)
        return flights

    def _retry_with_other_option(self, url):
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_airlines_current_flights(self, airlines: List[dict]):
        futures = generate_futures(
            self.get_airline_current_flights, [(airline) for airline in airlines]
        )

        results: List[dict] = []
        for future in as_completed(futures):
            try:
                args = futures[future]
                result = future.result()
                results.append(result)
            except requests.exceptions.HTTPError as e:
                url = e.request.url
                result = self._retry_with_other_option(url)
            except Exception as e:
                print(f"Failed to get detail for : {args} : {e}")

        return results
