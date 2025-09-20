#!/bin/bash

set -e

# Cleaning up any exiting PID files
echo "Cleaning up any existing airflow process"
# true means there is more to run in this file (if no file found then it throws error `true` ensures that it does break)
pkill -f "airflow webserver" || true
pkill -f "airflow schedular" || true
rm -f /opt/airflow/airflow-webserver.pid
rm -f /opt/airflow/airflow-schedular.pid

# Wait for processes to fully terminate
sleep 5


# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db init


# Create admin user
echo "Creating admin user..."

airflow users list | grep -q "^admin$" || airflow users create \
--username admin \
--firstname admin \
--lastname user \
--role Admin \
--email admin@example.com \
--password admin

# Start webserver and scheduler
echo "Starting airflow webserver and scheduler..."
airflow webserver --port 9099 --daemon &
airflow scheduler