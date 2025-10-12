import argparse
import os
import logging
from typing import Any, Dict, List, Tuple

from pyspark.sql import SparkSession
import psycopg2 as pg

"""
- create spark session
- Read data from s3
- Convert to table structure
- Create a staging table in postgres
- Load data into postgres staging table
- Perform upsert from staging to main table
- Cleanup staging table
- Log Success/Failure in file ingestion table
- Log processing log in process_run_log table
"""


# logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_env():
    from dotenv import load_dotenv

    loaded = load_dotenv()
    logger.info(f"Env loaded : {loaded}")
    return loaded


def get_spark_session(
    appName: str, region: str, aws_access_key: str, aws_secret_key: str
) -> SparkSession:
    spark: SparkSession = (
        SparkSession.builder.appName(appName)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    return spark


def read_s3_data(spark: SparkSession, s3_path: str):
    logger.info(f"Reading source data from {s3_path}")
    df = spark.read.option("multiline", "true").json(s3_path)
    return df


def read_monitoring_data(spark: SparkSession, pg_options: dict):
    logger.info("Reading data from monitoring.process_run_log table...")
    df = (
        spark.read.format("jdbc")
        .option("url", pg_options["url"])
        .option("dbtable", pg_options["dbtable"])
        .option("user", pg_options["user"])
        .option("password", pg_options["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    logger.info(f"Schema of monitoring.process_run_log: {df.schema}")
    df.show(5)


def get_update_query(
    schema: str,
    table_name: str,
    column_values: Dict[str, Any],
    conditions: Tuple[Tuple[str, str, Any], ...],
) -> Tuple[str, List[Any]]:
    """
    Supports any number of columns and conditions combined with AND.

    Returns:
        sql (str): Parameterized SQL query
        values (List[Any]): List of values for the placeholders
    """
    # SET clause
    set_clause = ", ".join([f"{col} = %s" for col in column_values.keys()])
    set_values = list(column_values.values())

    # WHERE clause
    where_clause_parts = []
    where_values = []
    for col, op, val in conditions:
        where_clause_parts.append(f"{col} {op} %s")
        where_values.append(val)
    where_clause = " AND ".join(where_clause_parts)

    sql = f"UPDATE {schema}.{table_name} SET {set_clause} WHERE {where_clause};"
    values = set_values + where_values

    return sql, values


def execute_query(pg_connection_config, query, values):
    connection = pg.connect(**pg_connection_config)
    cursor = connection.cursor()
    cursor.execute(query, values)
    connection.commit()
    connection.close()


def main():
    try:
        spark = None
        TASK_ID = "airlines_processor"
        logger.info("Starting ETL job...")
        loaded = load_env()
        if not loaded:
            raise EnvironmentError("Failed to load .env file")
        parser = argparse.ArgumentParser(description="ETL job: S3 JSON.GZ → Postgres")
        parser.add_argument("--s3_uri", required=True, help="s3 uri stored in database")
        parser.add_argument("--job_name", required=True, help="Spark Application Name")
        parser.add_argument("--run_id", required=True)
        parser.add_argument("--file_log_id", required=True)

        logger.info("Loading environment variables...")

        HOST = "postgres"  # container name
        PORT = 5432
        USER = os.getenv("POSTGRES_USER")
        PASSWORD = os.getenv("POSTGRES_PASSWORD")
        DB = os.getenv("POSTGRES_DB")
        aws_secret_key = os.getenv("AWS_SECRET_KEY")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        s3_bucket = os.getenv("S3_BUCKET")
        region = os.getenv("REGION")

        pg_connection_config = {
            "host": HOST,
            "port": PORT,
            "dbname": DB,
            "user": USER,
            "password": PASSWORD,
        }

        args = parser.parse_args()

        appName = args.job_name
        s3_key = args.s3_uri
        run_id = args.run_id
        file_log_id = args.file_log_id

        # Get update query for process_run_log table
        logger.info("Preparing to update process_run_log")
        query, values = get_update_query(
            "monitoring",
            "process_run_log",
            {"status": "Running", "task": f"{TASK_ID}"},
            (("id", "=", run_id),),
        )

        execute_query(pg_connection_config, query, values)
        logger.info("Updated process_run_log...")

        spark = get_spark_session(
            appName,
            region,
            aws_access_key,
            aws_secret_key,
        )

        PG_JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"
        logger.info("Spark session created.")
        s3_path = f"s3a://{s3_bucket}/{s3_key}"

        logger.info("Reading source data...")
        df = read_s3_data(spark, s3_path)
        logger.info(f"Got source data with share : {df.shape}...")

        PG_CONNECTION_PROPERTIES = {
            "user": USER,
            "password": PASSWORD,
            "driver": "org.pos tgresql.Driver",
        }

        logger.info("Reading data from monitoring.process_run_log table...")

        return
        pg_options = {
            "url": PG_JDBC_URL,
            "dbtable": "monitoring.process_run_log",
            "user": USER,
            "password": PASSWORD,  # Ideally, fetch this from a secret manager
            "driver": "org.postgresql.Driver",
        }
        read_monitoring_data(spark, pg_options)

        return

    except Exception as e:
        logger.error(f"Failed to complete job : {e.__class__} :{e}")
    finally:
        logger.info("Stopping spark session...")
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
