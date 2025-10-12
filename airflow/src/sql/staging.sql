CREATE SCHEMA IF NOT EXISTS staging;

-- Table: staging_airlines
CREATE TABLE
    IF NOT EXISTS staging.airline_staging (
        icao VARCHAR PRIMARY KEY,
        iata VARCHAR,
        name VARCHAR,
        short VARCHAR
    );