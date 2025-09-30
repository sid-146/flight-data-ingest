Todo:

-   Alembic creates tables every time: Observe behavior. (removed this headache, maybe in future)
    -   When container is stopped, and started again using compose start it gives error that commit not found (I guess pid is not deleted and when used down it is removed. and after strting again it works.)
    -   add db init file to initialize database while creating container
-   Postgres data not persistant after compose down, even after creating volume
-   using compose start sometimes airflow server does starts. Container is online but not able to access airflow.

Todo: Code
- Add logger please 😭😭

# Execution Plan

-   ## Stage One setup [-] Done
    -   Setup Airflow
    -   Setup db
    -   setup s3
-   ## Stage Two Ingestion Script
    -   Flight Client Handler
    -   Ingestion Handler
    -   Database Client Handler (Base and schema wise handler)
    -   DAG Creation and scheduling
