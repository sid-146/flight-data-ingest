## **1. Environment Setup**

### **Airflow Setup with Docker**

-   [-] completed

1. Install **Docker** and **Docker Compose** on your machine.
2. Use the official `docker-compose.yaml` from the Airflow project to bring up the Airflow environment.

    - Services to be included: `airflow-webserver`, `airflow-scheduler`, `airflow-worker`, `postgres` (for Airflow metadata), and possibly `redis` or `celery` if you want distributed execution.

3. Configure volume mounts so that your DAGs and plugins can be synced from a local folder (e.g., `./dags:/opt/airflow/dags`).
4. Start the Airflow cluster with `docker-compose up`.

---

## **2. Connect Airflow to GitHub Repository**

-   [-] Pending Need to create a new repo.

1. Create a GitHub repository where you’ll store DAGs, SQL scripts, and documentation.
2. Configure your Airflow container to pull DAGs from GitHub:

    - Use either a **git-sync sidecar container** or set up a CI/CD pipeline that deploys DAGs into the Airflow DAGs directory.
    - This ensures changes in GitHub are reflected inside Airflow automatically.

---

## **3. PostgreSQL Database Setup**

1. Spin up a **PostgreSQL container** alongside Airflow or use a managed Postgres instance.
2. Create a new database (e.g., `flights_db`).
3. Define schema for storing flight data. Example logical design:

    - **Raw Flights Table** (`flights_raw`): stores raw extracted data.

        - `flight_id`, `callsign`, `origin`, `destination`, `latitude`, `longitude`, `altitude`, `speed`, `timestamp`

    - **Reference Table** (`airports`): static info on airports for joins.

        - `airport_id`, `name`, `country`, `latitude`, `longitude`

    - **Clean Flights Table** (`flights_clean`): curated data after transformations.
    - **Aggregated Tables**:

        - `daily_flight_counts` → flights per day per origin/destination.
        - `average_speed_by_airline` → mean speed grouped by airline.

---

## **4. DAG Design in Airflow**

Break the workflow into tasks that reflect **core Airflow concepts**:

1. **Start DAG** → simple dummy operator as entry point.
2. **Extract Flight Data**

    - Use the **flightradar API/package** to pull data for flights in a specific region or globally.
    - Store JSON/CSV temporarily in staging (local or S3).

3. **Load Raw Data into Postgres**

    - Insert extracted data into `flights_raw`.

4. **Transform Data**

    - Clean duplicates, filter out incomplete records.
    - Enrich with airport info (joining `flights_raw` with `airports`).
    - Load results into `flights_clean`.

5. **Create Aggregates**

    - Generate daily metrics such as flight counts and average speeds.
    - Store them in `daily_flight_counts` and `average_speed_by_airline`.

6. **Validation & Quality Checks**

    - Ensure no NULL values in critical fields.
    - Verify row counts are consistent between raw and clean tables.

7. **End DAG** → log completion or send a notification.

---

## **5. Scheduling & Orchestration**

-   Schedule the DAG to run **every 30 minutes** for near real-time flight tracking, or once a day if you want to aggregate historical data.
-   Use **Airflow Variables** to store parameters like region of interest, or date ranges.
-   Use **Airflow Connections** for Postgres credentials and any API keys.

---

## **6. Monitoring & Logging**

-   Use Airflow’s built-in UI to track task success/failure.
-   Enable logs inside tasks for debugging extraction or loading issues.
-   Optionally, configure alerting (e.g., Slack or email operator) when a DAG fails.

---

## **7. Extension & Added Complexity**

1. **Standard Tables**: Add dimension tables such as `airlines`, `countries`, `routes`.
2. **Aggregates**:

    - Top 10 busiest airports per day.
    - Delay analysis if time-series data is available.

3. **Historical Storage**: Partition `flights_clean` by date for efficient querying.
4. **Dashboards**: Use a BI tool (e.g., Metabase, Superset, PowerBI) to connect to Postgres and visualize trends.

---

This approach ensures you cover **core Airflow concepts**:

-   DAG scheduling
-   Task dependencies
-   Connections to external services (Postgres, API)
-   Data validation and transformations
-   CI/CD integration with GitHub
