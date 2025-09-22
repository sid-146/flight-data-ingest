from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Boolean,
    Float,
    BigInteger,
)
from sqlalchemy.orm import relationship

from .config import BASE


class Flights(BASE):
    __tablename__ = "flights"
    __table_args__ = {"schema": "flight_data"}

    id: str = Column(String, nullable=False, primary_key=True)
    status: bool = Column(Boolean, nullable=True, comment="Status of current flight.")

    # Note: Foreign Keys need to reference the schema-qualified table name
    origin_airport_icao_code = Column(
        String, ForeignKey("flight_data.airport.icao_code")
    )
    destination_airport_icao_code = Column(
        String, ForeignKey("flight_data.airport.icao_code")
    )
    aircraft_id = Column(String, ForeignKey("flight_data.aircraft.registration"))

    origin_airport = relationship(
        "Airport",
        foreign_keys=[origin_airport_icao_code],
        back_populates="origin_flights",
    )
    destination_airport = relationship(
        "Airport",
        foreign_keys=[destination_airport_icao_code],
        back_populates="destination_flights",
    )
    aircraft = relationship("Aircraft", back_populates="flights")
    current_status = relationship(
        "FlightCurrentStatus", uselist=False, back_populates="flights"
    )
    trails = relationship("Trail", back_populates="flights")


class Airport(BASE):
    __tablename__ = "airport"
    __table_args__ = {"schema": "flight_data"}

    icao_code = Column(String, primary_key=True)
    iata_code = Column(String)
    latitude = Column(Float)
    longitude = Column(Float)
    altitude = Column(Float)
    city = Column(String)
    country = Column(String)
    country_code = Column(String)
    timezone = Column(String)
    offset = Column(Integer)
    offset_hours = Column(Float)

    # when accessed Airport.origin_flights it will join with flights
    #  to get all flights originated from this airport
    origin_flights = relationship(
        "Flights",
        foreign_keys="[Flights.origin_airport_icao_code]",
        back_populates="origin_airport",
    )
    destination_flights = relationship(
        "Flights",
        foreign_keys="[Flights.destination_airport_icao_code]",
        back_populates="destination_airport",
    )


class Airline(BASE):
    __tablename__ = "airline"
    __table_args__ = {"schema": "flight_data"}

    icao = Column(String, primary_key=True)
    iata = Column(String)
    name = Column(String)
    short = Column(String)

    aircrafts = relationship("Aircraft", back_populates="airline")


class Aircraft(BASE):
    __tablename__ = "aircraft"
    __table_args__ = {"schema": "flight_data"}

    registration = Column(String, primary_key=True)
    airline_id = Column(String, ForeignKey("flight_data.airline.icao"))
    code = Column(String)
    name = Column(String)

    airline = relationship("Airline", back_populates="aircrafts")
    images = relationship("Images", back_populates="aircraft")
    flights = relationship("Flights", back_populates="aircraft")


class Images(BASE):
    __tablename__ = "images"
    __table_args__ = {"schema": "flight_data"}

    url = Column(String, primary_key=True)
    src = Column(String)
    type = Column(String)
    copyright = Column(String)
    source = Column(String)
    aircraft_id = Column(String, ForeignKey("flight_data.aircraft.registration"))

    aircraft = relationship("Aircraft", back_populates="images")


class FlightCurrentStatus(BASE):
    __tablename__ = "flight_current_status"
    __table_args__ = {"schema": "flight_data"}

    flight_id = Column(String, ForeignKey("flight_data.flights.id"), primary_key=True)
    live = Column(Boolean)
    status = Column(String)
    type = Column(String)
    event_ts_utc = Column(BigInteger)

    flight = relationship("Flights", back_populates="current_status")


class Trail(BASE):
    __tablename__ = "trail"
    __table_args__ = {"schema": "flight_data"}

    # Since trail has a composite primary key, we specify them together
    flight_id = Column(String, ForeignKey("flight_data.flights.id"), primary_key=True)
    event_ts_utc = Column(BigInteger, primary_key=True)

    latitude = Column(Float)
    longitude = Column(Float)
    alt = Column(Float)

    flight = relationship("Flights", back_populates="trails")
