# Architecture

## **1. DAG 1 – Ingestion (Bronze Layer)**

-   **Purpose**: Collect raw data from FlightRadar API at regular intervals.
-   **Schedule**: Every 15–30 minutes (or whatever freshness you want).
-   **Tasks**:

    1. **Extract Flights**: Call the `flightradar` package to pull flight data.
    2. **Write Raw File to S3**: Save JSON/CSV exactly as received (no transformation yet). File naming pattern like:

        ```
        s3://flights-data/bronze/YYYY/MM/DD/flights_20250920_1200.json
        ```

    3. **Log Metadata**: Store metadata (file name, timestamp, record count) in Postgres for tracking.

✅ At this point, Bronze = **raw immutable data**.

---

## **2. DAG 2 – Processing with Sensor (Silver Layer)**

-   **Purpose**: Transform raw flight data into cleaned, structured tables.
-   **Trigger**: Not time-based. Instead, uses an **S3KeySensor** or **S3PrefixSensor**.

    -   Waits for a new file in `flights-data/bronze/`.

-   **Tasks**:

    1. **S3 Sensor**: Detects when a new raw file is created.
    2. **Trigger Spark Job**:

        - Spark reads the raw file from S3.
        - Cleans duplicates, handles missing values.
        - Joins with reference datasets (airports, airlines).
        - Converts into structured schema (e.g., Postgres `flights_clean`).

    3. **Load to Postgres**: Insert transformed records into the Silver layer (`flights_clean`).

✅ At this point, Silver = **clean, query-ready data**.

---

## **3. DAG 3 – Aggregation (Gold Layer)**

-   **Purpose**: Generate business-friendly aggregated tables for analytics.
-   **Schedule**: Daily or hourly, depending on needs.
-   **Tasks**:

    1. **Check Silver Table Freshness** (optional sensor, e.g., `ExternalTaskSensor` to ensure DAG 2 finished).
    2. **Aggregate Queries**: Run SQL transformations in Postgres to build Gold tables:

        - `daily_flight_counts` → flights per airport/day.
        - `average_speed_by_airline` → airline-level KPIs.
        - `busiest_routes` → origin-destination ranking.

    3. **Update Golden Tables**: Store results in Postgres under `gold` schema.

✅ At this point, Gold = **business KPIs and curated datasets**.

---

## **4. Flow in Medallion Terms**

-   **Bronze (Raw)** → Direct API dumps stored in S3.
-   **Silver (Cleaned)** → Structured, reliable data stored in Postgres (fact + dimension style).
-   **Gold (Curated)** → Aggregated metrics, ready for dashboards or BI tools.

---

## **5. Airflow Concepts Covered**

-   **Multiple DAGs** (3 clear stages).
-   **Sensors** (S3KeySensor waiting for raw files).
-   **ExternalTaskSensor** (optional, to coordinate DAG dependencies).
-   **SparkSubmitOperator** (trigger Spark job for heavy lifting).
-   **PostgresOperator** (to create/update Gold tables).
-   **Scheduling vs. Event-driven mix** (Ingestion = scheduled, Processing = sensor-triggered, Aggregation = scheduled).

---

This design is **production-like** and demonstrates:

-   Modular orchestration (each layer has its own DAG).
-   Integration with Spark + S3 + Postgres.
-   Bronze–Silver–Gold best practice for data engineering pipelines.

## Diagrams

### Sequence Diagram

```mermaid

sequenceDiagram
    participant API as FlightRadar API
    participant DAG1 as DAG 1: Ingestion (Bronze)
    participant S3 as S3 (Bronze Storage)
    participant Sensor as S3KeySensor
    participant DAG2 as DAG 2: Processing (Silver)
    participant Spark as Spark Job
    participant PG_Silver as Postgres (Silver Tables)
    participant DAG3 as DAG 3: Aggregation (Gold)
    participant PG_Gold as Postgres (Gold Tables)
    participant BI as BI/Analytics Tools

    API->>DAG1: Extract flight data
    DAG1->>S3: Store raw JSON/CSV
    S3-->>Sensor: Detect new file
    Sensor->>DAG2: Trigger processing
    DAG2->>Spark: Run data cleaning & joins
    Spark->>PG_Silver: Load into flights_clean
    PG_Silver-->>DAG3: Available for aggregation
    DAG3->>PG_Gold: Create aggregated tables
    PG_Gold->>BI: Serve dashboards & reports

```

### Activity Diagram

```mermaid

flowchart TD
    subgraph DAG1["DAG 1: Ingestion (Bronze)"]
        A1["Start Scheduled: every 15-30 min"]
        A2["Extract data from FlightRadar API"]
        A3["Store raw JSON/CSV in S3 (Bronze)"]
        A4["Log metadata in Postgres"]
        A1 --> A2 --> A3 --> A4
    end

    subgraph DAG2["DAG 2: Processing (Silver)"]
        B1["S3KeySensor: Wait for new file"]
        B2["Trigger Spark job"]
        B3["Clean & enrich data"]
        B4["Load into Postgres flights_clean (Silver)"]
        B1 --> B2 --> B3 --> B4
    end

    subgraph DAG3["DAG 3: Aggregation (Gold)"]
        C1["Daily/Hourly Schedule"]
        C2["Check Silver table freshness"]
        C3["Run Aggregation Queries"]
        C4["Update Postgres Gold Tables"]
        C5["Serve BI dashboards"]
        C1 --> C2 --> C3 --> C4 --> C5
    end

    %% Cross-DAG dependencies
    A3 --> B1
    B4 --> C2

```



```mermaid
flowchart TD

%% ================================
%% DAG 1 – Ingestion (Bronze Layer)
%% ================================
subgraph DAG1 ["Ingestion DAG (Bronze)"]
    A1["Start"]
    A2["Extract Flights from API"]
    A3["Validate Extraction"]
    A4["Store to S3 (Bronze)"]
    A5["Log Metadata (Postgres)"]
    A6["End"]

    A1 --> A2 --> A3 --> A4 --> A5 --> A6
end

%% ================================
%% DAG 2 – Processing (Silver Layer)
%% ================================
subgraph DAG2 ["Processing DAG (Silver)"]
    B1["Wait for File (S3 Sensor)"]
    B2["Trigger Spark Job"]
    B3["Read Raw Data"]
    B4["Clean Data"]
    B5["Enrich with Airports/Airlines"]
    B6["Write to Postgres flights_clean"]
    B7["Quality Checks"]
    B8["Update Metadata"]

    B1 --> B2 --> B3 --> B4 --> B5 --> B6 --> B7 --> B8
end

%% ================================
%% DAG 3 – Aggregation (Gold Layer)
%% ================================
subgraph DAG3 ["Aggregation DAG (Gold)"]
    C1["Start"]
    C2["Check Silver Freshness"]
    C3["Aggregate Daily Flights"]
    C4["Aggregate Airline Speed"]
    C5["Top Routes & Busiest Airports"]
    C6["Data Quality Checks"]
    C7["Update Metadata"]
    C8["End"]

    C1 --> C2 --> C3 --> C4 --> C5 --> C6 --> C7 --> C8
end

%% ================================
%% Cross-DAG Dependencies
%% ================================
A6 -->|Raw Data in S3| B1
B8 -->|Flights Clean in Postgres| C2

```