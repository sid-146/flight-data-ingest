#!/bin/bash
set -e

# Start Spark based on the SPARK_MODE environment variable
if [ "$SPARK_MODE" == "master" ]; then
    echo "Starting Spark master..."
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master
    elif [ "$SPARK_MODE" == "worker" ]; then
    echo "Starting Spark worker..."
    $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker $SPARK_MASTER_URL
else
    echo "Error: SPARK_MODE environment variable not set or invalid."
    echo "Valid values are 'master' or 'worker'."
    exit 1
fi