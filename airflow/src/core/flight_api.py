# Sort import properly
import time
import random
import traceback
from abc import ABC
import dataclasses

import requests
from typing import List, Literal
from concurrent.futures import as_completed

from FlightRadar24.api import FlightRadar24API, Flight, Countries


from src.core.logger import console
from src.core.utils import generate_futures, get_content


from fp.fp import FreeProxy


# Flight Current Data: https://data-live.flightradar24.com/clickhandler/?flight=3c75b726
# airlines_data


"""
What to do:
    - get airlines using FlightRadarAPI
    - clean airlines (remove navy, air force, army, school, university)
    - Iterate over all airlines get list of airflow using proxy rotation. -> returns List[Flight]
    - Iterate over each flight and get flight details using proxy rotation. -> returns List[Details]
    - Store Data in data.json.gz
"""


class Core(ABC):
    # Base URLs
    api_flightradar_base_url = "https://api.flightradar24.com/common/v1"
    cdn_flightradar_base_url = "https://cdn.flightradar24.com"
    flightradar_base_url = "https://www.flightradar24.com"
    data_live_base_url = "https://data-live.flightradar24.com"
    data_cloud_base_url = "https://data-cloud.flightradar24.com"

    # Flight Data URL
    real_time_flight_tracker_data_url = data_cloud_base_url + "/zones/fcgi/feed.js"
    flight_data_url = data_live_base_url + "/clickhandler/?flight={}"

    # Airport URLs
    api_airport_data_url = api_flightradar_base_url + "/airport.json"
    airport_data_url = flightradar_base_url + "/airports/traffic-stats/?airport={}"
    airports_data_url = flightradar_base_url + "/data/airports"

    headers = {
        "accept-encoding": "gzip, br",
        "accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "origin": "https://www.flightradar24.com",
        "referer": "https://www.flightradar24.com/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
    }


class FlightApiClient:
    def __init__(self):
        self.api = FlightRadar24API()
        # Todo:
        # update this code to get a working proxy list.
        # Sometime is getting Connection timeout error
        self.proxies = FreeProxy().get_proxy_list(repeat=1)

    def get_airlines(self):
        airlines = self.api.get_airlines()
        return airlines

    def _airline_criteria_(airline):
        name = airline["Name"].lower()
        if (
            "army" in name
            or "force" in name
            or "navy" in name
            or "school" in name
            or "university" in name
        ) and airline["n_aircrafts"] > 20:
            return airline

    def __make_api_call(
        session: requests.Session,
        method: Literal["POST", "GET"],
        url,
        headers,
        params,
        proxy,
    ):
        response = session.request(
            method,
            url,
            params=params,
            headers=headers,
            proxies=proxy,
        )
        return response

    def get_airline_flights(self, airlines):

        console.info(f"Airlines before filtering : {len(airlines)}")
        airlines = list(filter(self._airline_criteria_, airlines))
        console.info(f"Airlines after filtering : {len(airlines)}")

        flights = []
        session = requests.Session()
        session.get(Core.flightradar_base_url)
        request_params = self.api.get_flight_tracker_config().__dict__
        url = Core.data_live_base_url + "/zones/fcgi/feed.js"

        args = []

        for airline in airlines:
            request_params["airline"] = airline
            _proxy = random.choice(self.proxies)
            args.append[
                (
                    session,
                    "GET",
                    url,
                    Core.headers,
                    request_params,
                    {"http": _proxy, "https": _proxy},
                )
            ]

        futures = generate_futures(self.__make_api_call, args)
        for future in as_completed(futures):
            try:
                response: requests.Response = future.result()
                response.raise_for_status()
                content = get_content(response)

                for flight_id, flight_info in content.items():
                    if not flight_id[0].isnumeric():
                        continue

                    flight = Flight(flight_id, flight_info)
                    flights.append(flight)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    console.error("Failed due to status code 429")
                console.error(f"HTTP Error : {e} : {e.__class__}")
                console.error(f"Args passed : {args}")
                console.warning("Decide what to do with error.")
            except Exception as e:
                console.error(f"Failed to get airline : {e.__class__} : {e}")
                console.error(f"Args passed : {args}")

        session.close()
        return flights

    def get_flight_details(self, flights: List[Flight]) -> List[dict]:
        # Build up args
        # Use proxies for each different request
        # Create Future
        # Iterate over futures as completed
        # get result from the future
        # Extract content from the future
        # Append to a list of details

        args = []
        session = requests.Session()
        session.get(Core.flightradar_base_url)
        params = dataclasses.asdict(self.api.get_flight_tracker_config())
        details = []

        for flight in flights:
            _proxy = random.choice(self.proxies)
            url = Core.flight_data_url.format(flight.id)
            args.append(
                (
                    "GET",
                    url,
                    Core.headers,
                    params,
                    {"http": _proxy, "https": _proxy},
                )
            )

        futures = generate_futures(self._make_api_call, args)
        for future in as_completed(futures):
            try:
                response: requests.Response = future.result()
                response.raise_for_status()
                content = get_content(response)
                details.append(content)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    console.error("Failed due to status code 429")
                console.error(f"HTTP Error : {e} : {e.__class__}")
                console.error(f"Args passed : {args}")
                console.warning("Decide what to do with error.")
            except Exception as e:
                console.error(f"Failed to get airline : {e.__class__} : {e}")
                console.error(f"Args passed : {args}")

        session.close()
        return details

    def get_airports(self, countries: List[Countries]):
        # Start with two or three countries, Slowly role out for more country.
        return self.api.get_airports(countries)

    def __is_valid_code(code):
        if 4 < len(code) or len(code) < 3:
            raise ValueError("The code : {code} is valid.")

    def get_airport_details(
        self,
        codes: str,
    ):
        # Build up args
        # Use proxies for each different request
        # Create Future
        # Iterate over futures as completed
        # get result from the future
        # Extract content from the future
        # Append to a list of details
        request_params = {"format": "json", "limit": 1, "page": 1}
        session = requests.Session()
        session.get(Core.flightradar_base_url)
        args = []
        for code in codes:
            self.__is_valid_code(code)
            request_params["code"] = code
            proxy = random.choice(self.proxies)
            args.append(
                [
                    session,
                    "GET",
                    Core.api_airport_data_url,
                    Core.headers,
                    request_params,
                    {"http": proxy, "https": proxy},
                ]
            )

        futures = generate_futures(self.__make_api_call, args)

        for future in as_completed(futures):
            # pass
            try:
                response: requests.Response = future.result()
                response.raise_for_status()
                content = get_content(response)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    console.error("Failed due to status code 429")
                console.error(f"HTTP Error : {e} : {e.__class__}")
                console.error(f"Args passed : {args}")
                console.warning("Decide what to do with error.")
            except Exception as e:
                console.error(f"Failed to get airline : {e.__class__} : {e}")
                console.error(f"Args passed : {args}")

        return

    # * New methods above this, below need to be removed

    # @staticmethod
    # def _flight_details(api: FlightRadar24API, flight):
    #     flight = api.get_flight_details(flight)
    #     return flight

    # @staticmethod
    # def get_airline_current_flights(api: FlightRadar24API, airline):
    #     flights = api.get_flights(airline["ICAO"])
    #     return flights

    # def _retry_with_other_option(self, url):
    #     response = requests.get(url)
    #     response.raise_for_status()
    #     return response.json()

    # def get_airlines_current_flights(self, airlines: List[dict]) -> List[Flight]:
    #     console.info(f"Getting all flights of : {len(airlines)}")
    #     futures = generate_futures(
    #         self.get_airline_current_flights,
    #         [(self.api, airline) for airline in airlines],
    #     )

    #     flights: List[dict] = []
    #     for future in as_completed(futures):
    #         try:
    #             args = futures[future]
    #             _flights = future.result()
    #             flights.extend(_flights)
    #         except requests.exceptions.HTTPError as e:
    #             if e.response.status_code == 429:
    #                 max_retries = 4
    #                 delay = 1  # 2 seconds delay
    #                 for i in range(max_retries):
    #                     console.warning(f"Rate Limit hit. {args[1]['ICAO']}")
    #                     time.sleep(delay)
    #                     _flights = self.get_airline_current_flights(
    #                         self.api, args[1]["ICAO"]
    #                     )
    #                     delay = (delay * 2) + (random.uniform(0, 1))
    #                     flights.append(_flights)
    #             else:
    #                 raise e
    #         except Exception as e:
    #             traceback.print_exc()
    #             console.error(f"Failed to get flight detail for : {args} : {e}")

    #     console.debug(f"flights : {len(flights)}")
    #     return flights

    # def get_flights_details(self, flights: List[Flight]):
    #     console.info(f"Getting flight details : {len(flights)}")
    #     futures = generate_futures(
    #         self.api.get_flight_details,
    #         [(flight) for flight in flights],
    #     )

    #     details: List[dict] = []
    #     for future in as_completed(futures):
    #         try:
    #             args = futures[future]
    #             _detail = future.result()
    #             details.append(_detail)
    #         except requests.exceptions.HTTPError as e:
    #             if e.response.status_code == 429:
    #                 max_retries = 4
    #                 delay = 1
    #                 for _ in range(max_retries):
    #                     console.warning(f"Rate Limit hit. {args}")
    #                     time.sleep(delay)
    #                     _detail = self.api.get_flight_details(args)
    #                     delay = (delay * 2) + random.uniform(0, 1)
    #                     details.append(_detail)
    #             else:
    #                 console.critical(
    #                     "Not able to get it through package, trying direct api."
    #                 )
    #                 url = e.request.url
    #                 _details = self._retry_with_other_option(url)
    #                 details.append(_details)
    #         except Exception as e:
    #             console.error(f"Failed to get detail for : {args} : {e}")

    #     return details
