Todo:

-   Alembic creates tables every time: Observe behavior. (removed this headache, maybe in future)
    -   When container is stopped, and started again using compose start it gives error that commit not found (I guess pid is not deleted and when used down it is removed. and after strting again it works.)
    -   add db init file to initialize database while creating container
-   Postgres data not persistant after compose down, even after creating volume
-   using compose start sometimes airflow server does starts. Container is online but not able to access airflow.

Todo: Code

-   [-] Add logger please 😭😭
-   [-] Create a container, if there is issue in calling flight api through container....
-   [-] Complete airport details function.
-   For starting keep the flights and airports counts low.
-   [-] Create/move function as such that they are more reuseable.
-   Sort imports
-   Airports details contains more information which is not present in current schema. (Thus Schema update is needed.)

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

# Dag to be created

-   ## airline updater dag

    -   ### Tasks
        -   Insert Process Log
        -   Fetch all airlines
        -   Insert/update in airlines table
        -   Update process log

-   ## Airport updater dag

    -   ### Tasks
        -   Insert Process Log
        -   Fetch all airports
        -   Insert/update in airports table
        -   Update process log

-   ## Data Ingestion Dag updater
    -   ### Tasks
        -   Insert Process Log
        -   Get all flights
        -   get all flight details
        -   Insert/Update in required tables
        -   Update process log
