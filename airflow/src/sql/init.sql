-- Create the schema
CREATE SCHEMA IF NOT EXISTS flight_data;

CREATE SCHEMA IF NOT EXISTS monitoring;

-- Table: airport
CREATE TABLE
    IF NOT EXISTS flight_data.airport (
        icao_code VARCHAR PRIMARY KEY,
        iata_code VARCHAR,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        altitude DOUBLE PRECISION,
        city VARCHAR,
        country VARCHAR,
        country_code VARCHAR,
        timezone VARCHAR,
        "offset" INTEGER,
        offset_hours DOUBLE PRECISION
    );

-- Table: airline
CREATE TABLE
    IF NOT EXISTS flight_data.airline (
        icao VARCHAR PRIMARY KEY,
        iata VARCHAR,
        name VARCHAR,
        short VARCHAR
    );

-- Table: aircraft
CREATE TABLE
    IF NOT EXISTS flight_data.aircraft (
        registration VARCHAR PRIMARY KEY,
        airline_id VARCHAR REFERENCES flight_data.airline (icao),
        code VARCHAR,
        name VARCHAR
    );

-- Table: images
CREATE TABLE
    IF NOT EXISTS flight_data.images (
        url VARCHAR PRIMARY KEY,
        src VARCHAR,
        type VARCHAR,
        copyright VARCHAR,
        source VARCHAR,
        aircraft_id VARCHAR REFERENCES flight_data.aircraft (registration)
    );

-- Table: flights
CREATE TABLE
    IF NOT EXISTS flight_data.flights (
        id VARCHAR PRIMARY KEY,
        status BOOLEAN,
        origin_airport_icao_code VARCHAR REFERENCES flight_data.airport (icao_code),
        destination_airport_icao_code VARCHAR REFERENCES flight_data.airport (icao_code),
        aircraft_id VARCHAR REFERENCES flight_data.aircraft (registration)
    );

-- Table: flight_current_status
CREATE TABLE
    IF NOT EXISTS if not exists flight_data.flight_current_status (
        flight_id VARCHAR PRIMARY KEY REFERENCES flight_data.flights (id),
        live BOOLEAN,
        status VARCHAR,
        type VARCHAR,
        event_ts_utc BIGINT
    );

-- Table: trail
CREATE TABLE
    IF NOT EXISTS flight_data.trail (
        flight_id VARCHAR REFERENCES flight_data.flights (id),
        event_ts_utc BIGINT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION,
        alt DOUBLE PRECISION,
        PRIMARY KEY (flight_id, event_ts_utc)
    );

CREATE TABLE
    IF NOT EXISTS monitoring.process_run_log (
        id BIGINT PRIMARY KEY,
        dag_name VARCHAR,
        starttime TIMESTAMP(3) default CURRENT_TIMESTAMP,
        status VARCHAR
    )