Todo: 
- Alembic creates tables every time: Observe behavior. (removed this headache, maybe in future)
    - When container is stopped, and started again using compose start it gives error that commit not found
    - add db init file to initialize database while creating container

# Execution Plan

-   ## Stage One setup [-] Done
    -   Setup Airflow 
    -   Setup db
    -   setup s3
-   ## Stage Two Ingestion Script
    -   Flight Client Handler
    -   Ingestion Handler
    -   DAG Creation and scheduling
