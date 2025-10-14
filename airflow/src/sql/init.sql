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
ADD COLUMN "errors" TEXT,
ADD COLUMN record_created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP;

-- Created Table ingestion file log
CREATE SEQUENCE IF NOT EXISTS monitoring.ingestion_file_log_id_seq INCREMENT BY 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1 NO CYCLE;

CREATE TABLE
    IF NOT EXISTS monitoring.ingestion_file_log (
        id BIGINT,
        run_id BIGINT REFERENCES monitoring.process_run_log (id),
        s3_key VARCHAR,
        no_records INTEGER,
        record_created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP
    );

ALTER TABLE monitoring.ingestion_file_log
ALTER COLUMN id
SET DEFAULT nextval ('monitoring.ingestion_file_log_id_seq');

ALTER TABLE monitoring.ingestion_file_log ADD CONSTRAINT ingestion_file_log_pkey PRIMARY KEY (id);

ALTER SEQUENCE monitoring.ingestion_file_log_id_seq OWNED BY monitoring.ingestion_file_log.id;

-- adding column for data_type in monitoring.ingestion_file_log table
ALTER TABLE monitoring.ingestion_file_log
ADD COLUMN data_type varchar,
ADD COLUMN is_processed varchar DEFAULT 'pending';

-- Alter airlines tables
ALTER TABLE flight_data.airline
ADD COLUMN IF NOT EXISTS record_created_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP,
ADD COLUMN IF NOT EXISTS record_updated_at TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP;

-- -- Added trigger function
-- CREATE
-- OR REPLACE FUNCTION flight_data.set_updated_at () RETURNS TRIGGER AS
-- -- $$
-- BEGIN NEW.updated_at = now ();
-- RETURN NEW;
-- END;
-- -- $$ LANGUAGE plpgsql;


-- Added Trigger
-- CREATE TRIGGER set_timestamp BEFORE
-- UPDATE ON flight_data.airlines FOR EACH ROW EXECUTE FUNCTION flight_data.set_updated_at ();
