# def main():
#     print("Hello from flight-data-ingest!")


# if __name__ == "__main__":
#     main()


from FlightRadar24.api import FlightRadar24API
from FlightRadar24 import Flight

from concurrent.futures import ThreadPoolExecutor, as_completed

import os
import json
from datetime import datetime
import time
from typing import List

import gzip

print("Starting..")


def compress(results, store_path):
    os.makedirs(store_path, exist_ok=True)
    with gzip.open(
        os.path.join(store_path, "data.json.gz"),
        "wt",
        encoding="utf-8",
        compresslevel=5,
    ) as f:
        # add hash with json for checking the data integrity.
        json.dump(results, f, indent=2)


api = FlightRadar24API()
config = api.get_flight_tracker_config()
config.limit = 20
api.set_flight_tracker_config(config)

print(api.get_flight_tracker_config())


def get_flight_details(flight):
    result = api.get_flight_details(flight)
    return result


counter = 0


def process(api: FlightRadar24API, get_flight_details: callable):
    airlines = api.get_airlines()[:20]
    flights: List[Flight] = []
    results = []

    for airline in airlines:
        print(f"Running : {airline['Name']} : {airline['n_aircrafts']}")
        _flights = api.get_flights(airline=airline["ICAO"])
        print(f"Got {len(_flights)} for {airline['Name']}")
        flights.extend(_flights)

    print(f"Length of flight list. : {len(flights)}")

    with ThreadPoolExecutor(max_workers=min(len(flights), 4)) as pool:
        futures = {
            pool.submit(get_flight_details, flight): flight for flight in flights
        }

    for future in as_completed(futures):
        args = futures[future]
        print(f"Completed for flight : {args}")
        result = future.result()
        results.append(result)

    return results


while True:
    folder_name = f"flights_data_{datetime.now().strftime("%y_%m_%d_%M")}"
    results = process(api, get_flight_details)
    print(f"Returned output length {len(results)}")

    store_path = os.path.join("store", folder_name)
    compress(results, store_path)
    # os.makedirs(store_path, exist_ok=True)
    # with open(os.path.join(store_path, "data.json"), "a") as f:
    #     json.dump(results, f, indent=2)

    if counter == 3:
        break
    else:
        counter += 1

    print(f"Processing Completed for iteration {counter}")
    print("sleeping for three minutes.")
    print()
    time.sleep(180)
