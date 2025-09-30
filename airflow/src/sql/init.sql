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
    IF NOT EXISTS flight_data.flight_current_status (
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

-- Create sequence for autoincrement.
CREATE SEQUENCE IF NOT EXISTS monitoring.process_run_log_id_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE;

-- 3. Create the Table (without a PRIMARY KEY defined here)
CREATE TABLE
    IF NOT EXISTS monitoring.process_run_log (
        id BIGINT, -- No PRIMARY KEY here yet
        dag_name VARCHAR,
        starttime TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR
    );

-- 4. Set the default value for the 'id' column to use the sequence (Autoincrement)
ALTER TABLE monitoring.process_run_log
ALTER COLUMN id
SET DEFAULT nextval ('monitoring.process_run_log_id_seq');

-- 5. Add the PRIMARY KEY constraint to the 'id' column
ALTER TABLE monitoring.process_run_log ADD CONSTRAINT process_run_log_pkey PRIMARY KEY (id);

-- Optional: You can "own" the sequence by the table column, which ensures 
-- that when the table or column is dropped, the sequence is dropped too.
ALTER SEQUENCE monitoring.process_run_log_id_seq OWNED BY monitoring.process_run_log.id;

-- Updating Table to add task and error columns.
ALTER TABLE monitoring.process_run_log
ADD COLUMN task varchar,
ADD COLUMN "errors" TEXT;
