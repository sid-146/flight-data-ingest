# def main():
#     print("Hello from flight-data-ingest!")


# if __name__ == "__main__":
#     main()


from FlightRadar24.api import FlightRadar24API
from FlightRadar24 import Flight

from concurrent.futures import ThreadPoolExecutor, as_completed

import os
from datetime import datetime
import time
from typing import List

print("Starting..")


api = FlightRadar24API()
config = api.get_flight_tracker_config()
config.limit = 20
api.set_flight_tracker_config(config)

print(api.get_flight_tracker_config())


def get_flight_details(flight):
    result = api.get_flight_details(flight)
    return result


counter = 0
while True:
    airlines = api.get_airlines()[:3]
    flights: List[Flight] = []
    folder_name = f"flights_data_{datetime.now().strftime("%y_%m_%d_%M")}"
    results = []

    for airline in airlines:
        print(f"Running : {airline['Name']} : {airline['n_aircrafts']}")
        _flights = api.get_flights(airline=airline["ICAO"])
        flights.extend(_flights)

    with ThreadPoolExecutor(max_workers=min(len(flights), 4)) as pool:
        futures = pool.submit()

    for future in as_completed(futures):
        result = future.result()
        results.append(result)

    store_path = os.path.join("store", folder_name)
    os.makedirs(store_path, exist_ok=True)

    print(f"processing {len(flights)}")

    if counter == 3:
        break
    else:
        counter += 1
    print("sleeping for three minutes.")
    print()
    time.sleep(180)
