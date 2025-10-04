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

        # self.proxies = self.__working_proxies()
        # self.proxies = (
        #     self.proxies if self.proxies else [FreeProxy().get() for i in range(100)]
        # )

        # Currently using built in proxy checker
        self.fp = FreeProxy()
        self.proxies = [self.fp.get() for _ in range(100)]

    def __working_proxies(self):
        session = requests.Session()
        session.get(Core.flightradar_base_url, headers=Core.headers)
        args = [
            (
                session,
                "GET",
                Core.flightradar_base_url,
                Core.headers,
                {},
                {"http": proxy, "https": proxy},
            )
            for proxy in FreeProxy().get_proxy_list(repeat=1)
        ]

        futures = generate_futures(FlightApiClient.__make_api_call, args)

        working_proxy = []
        for future in as_completed(futures):
            try:
                response: requests.Response = future.result()
                response.raise_for_status()
                if response.status_code == 200:
                    working_proxy.append(args[-1]["http"])
                else:
                    console.warning(
                        f"Something went wrong while testing proxy. Response Content : {response.content}"
                    )

            except (requests.exceptions.HTTPError, requests.exceptions.Timeout) as e:
                console.error(f"Failed HTTP request : {e.__class__} : {e} : {args[-1]}")

            except Exception as e:
                console.error(f"Proxy testing failed for : {args[-1]} : {e}")

        return working_proxy

    @staticmethod
    def _airline_criteria_(airline):
        name = airline["Name"].lower()
        if (
            not (
                "army" in name
                or "force" in name
                or "navy" in name
                or "school" in name
                or "university" in name
            )
            and airline["n_aircrafts"] > 20
        ):
            return airline

    def get_airlines(self):
        airlines = self.api.get_airlines()
        console.info(f"Airlines before filtering : {len(airlines)}")
        airlines = list(filter(FlightApiClient._airline_criteria_, airlines))
        console.info(f"Airlines after filtering : {len(airlines)}")
        return airlines

    @staticmethod
    def __make_api_call(
        session: requests.Session,
        method: Literal["POST", "GET"],
        url,
        headers,
        params,
        proxy,
    ):
        url = url + "?" + "&".join(["{}={}".format(k, v) for k, v in params.items()])
        response = session.request(
            method=method,
            url=url,  # Todo: url might be causing issue of not returning any flights
            # params=params,
            headers=headers,
            proxies=proxy,
        )
        return response

    def get_airline_flights(self, airlines):
        flights = []
        session = requests.Session()
        session.get(Core.flightradar_base_url, headers=Core.headers)
        request_params = self.api.get_flight_tracker_config().__dict__
        url = Core.data_live_base_url + "/zones/fcgi/feed.js"
        args = []
        for airline in airlines:
            request_params["airline"] = airline["ICAO"]
            print(request_params)
            # _proxy = random.choice(self.proxies)
            _proxy = self.fp.get()
            args.append(
                (
                    session,
                    "GET",
                    url,
                    Core.headers,
                    request_params,
                    {"http": _proxy, "https": _proxy},
                )
            )

        futures = generate_futures(FlightApiClient.__make_api_call, args)
        for future in as_completed(futures):
            try:
                args = futures[future]
                response: requests.Response = future.result()
                print(response.url)
                response.raise_for_status()
                console.info(f"Completed for airline : {args[-2]['airline']}")
                content = get_content(response)

                for flight_id, flight_info in content.items():
                    print(flight_info)
                    if not flight_id[0].isnumeric():
                        continue

                    flight = Flight(flight_id, flight_info)
                    flights.append(flight)
                    print(flight)
            except requests.exceptions.HTTPError as e:
                console.warning(f"Got Status Code : {e.response.status_code}")
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
        args = []
        session = requests.Session()
        session.get(Core.flightradar_base_url, headers=Core.headers)
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

        futures = generate_futures(FlightApiClient.__make_api_call, args)
        for future in as_completed(futures):
            try:
                response: requests.Response = future.result()
                response.raise_for_status()
                content = get_content(response)
                details.append(content)
            except requests.exceptions.HTTPError as e:
                console.warning(f"Got Status Code : {e.response.status_code}")
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

    def get_airport_details(self, codes: str):
        request_params = {"format": "json", "limit": 1, "page": 1}
        session = requests.Session()
        session.get(Core.flightradar_base_url, headers=Core.headers)
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

        futures = generate_futures(FlightApiClient.__make_api_call, args)

        for future in as_completed(futures):
            try:
                args = futures[future]
                response: requests.Response = future.result()
                response.raise_for_status()
                content = get_content(response)
                data: dict = content["result"]["response"]
                data = data.get("airport", {}).get("pluginData", {})
                if (
                    "details" not in data
                    and len(data.get("runways", [])) == 0
                    and len(data) <= 3
                ):
                    raise ValueError(f"Did not find any airport with code : {args[-2]}")
            except requests.exceptions.HTTPError as e:
                console.warning(f"Got Status Code : {e.response.status_code}")
                if e.response.status_code == 429:
                    console.error("Failed due to status code 429")
                console.error(f"HTTP Error : {e} : {e.__class__}")
                console.error(f"Args passed : {args}")
                console.warning("Decide what to do with error.")
            except Exception as e:
                console.error(f"Failed to get airline : {e.__class__} : {e}")
                console.error(f"Args passed : {args}")

        session.close()
        return
