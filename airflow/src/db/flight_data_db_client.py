from src.db.BaseDBClient import BaseDBClient
from src.db.models.flight_models import Flights, Aircraft, Airline, Airport


class FlightDBClient(BaseDBClient):
    def __init__(self):
        self._schema = "flight_data"

    def get_flights(self, **kwargs):
        return self.query(Flights, **kwargs)

    def insert_flight(self, FlightObject, **kwargs):
        # todo: make this controller (get dictionary as input and generate obj to insert)
        self.insert(FlightObject)

    def get_airports(self, **kwargs):
        return self.query(Airport)
