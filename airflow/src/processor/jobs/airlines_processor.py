import argparse
import os
import logging
import traceback
from typing import Any, Dict, List, Tuple

from pyspark.sql import SparkSession, DataFrame
import psycopg2 as pg

from pydantic import computed_field
from pydantic_settings import BaseSettings

"""
- [x] create spark session
- [x] Read data from s3
- [x] Convert to table structure
- [x] Create a staging table in postgres
- Load data into postgres staging table
- Perform upsert from staging to main table
- Cleanup staging table
- Log Success/Failure in file ingestion table
- Log processing log in process_run_log table
"""


# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


class Config(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    AWS_SECRET_KEY: str
    AWS_ACCESS_KEY_ID: str
    S3_BUCKET: str
    REGION: str

    @computed_field
    @property
    def SPARK_JDBC_URL(self) -> str:
        return f"jdbc:postgresql://{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @computed_field
    @property
    def PG_CONNECTION_CONFIG(self) -> dict:
        return {
            "host": self.POSTGRES_HOST,
            "port": self.POSTGRES_PORT,
            "dbname": self.POSTGRES_DB,
            "user": self.POSTGRES_USER,
            "password": self.POSTGRES_PASSWORD,
        }

    @computed_field
    @property
    def SPARK_PG_CONNECTION_CONFIG(self) -> dict:
        """Need to add db table key."""
        return {
            "url": self.SPARK_JDBC_URL,
            "user": self.POSTGRES_USER,
            "password": self.POSTGRES_PASSWORD,  # Ideally, fetch this from a secret manager
            "driver": "org.postgresql.Driver",
        }


def load_env():
    from dotenv import load_dotenv

    loaded = load_dotenv()
    print(f"Env loaded : {loaded}")
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
    print(f"Reading source data from {s3_path}")
    df = spark.read.option("multiline", "true").json(s3_path)
    return df


def read_monitoring_data(spark: SparkSession, pg_options: dict):
    print("Reading data from monitoring.process_run_log table...")
    df = (
        spark.read.format("jdbc")
        .option("url", pg_options["url"])
        .option("dbtable", pg_options["dbtable"])
        .option("user", pg_options["user"])
        .option("password", pg_options["password"])
        .option("driver", "org.postgresql.Driver")
        .load()
    )

    print(f"Schema of monitoring.process_run_log: {df.schema}")


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


def create_staging_table(pg_connection_config, schema, table):
    query = """
SELECT
    column_name,
    udt_name as data_type
FROM
    information_schema.columns
WHERE
    table_schema = %s
    AND table_name = %s;
    """

    result = execute_select_query(pg_connection_config, query, [schema, table])
    column_definitions = [
        f"{column_name} {data_type}" for column_name, data_type in result
    ]
    query = f"""
    CREATE SCHEMA IF NOT EXISTS staging;

    CREATE TABLE IF NOT EXISTS staging.{table} (
        {',\n'.join(column_definitions)}
    );
    """

    execute_dml_ddl_query(pg_connection_config, query)

    return result


def execute_dml_ddl_query(pg_connection_config, query, values=None):
    connection = pg.connect(**pg_connection_config)
    cursor = connection.cursor()
    if values:
        cursor.execute(query, values)
    else:
        cursor.execute(query)
    connection.commit()
    connection.close()


def execute_select_query(pg_connection_config, query, values=None):
    connection = pg.connect(**pg_connection_config)
    cursor = connection.cursor()
    if values:
        cursor.execute(query, values)
    else:
        cursor.execute(query)

    result = cursor.fetchall()
    connection.commit()
    connection.close()

    return result


def write_staging(pg_config: dict, df: DataFrame):
    df.write.format("jdbc").option("url", pg_config["url"]).option(
        "dbtable", pg_config["dbtable"]
    ).option("user", pg_config["user"]).option(
        "password", pg_config["password"]
    ).option(
        "driver", pg_config["driver"]
    ).mode(
        "overwrite"
    ).save()


def perform_upsert_query(
    from_table: str,
    from_schema: str,
    to_table: str,
    to_schema: str,
    pk_columns: Tuple[str],
    columns: Tuple[str],
    pg_connection_config: dict,
):
    query = f"""
INSERT INTO 
    {to_schema}.{to_table}
SELECT 
    {',\n'.join(columns)} 
FROM 
    {from_schema}.{from_table}
ON CONFLICT ({','.join(pk_columns)}) DO 
UPDATE 
SET
    {',\n'.join([f"{col}=EXCLUDED.{col}" for col in columns if col not in pk_columns])}
"""
    print(query)
    execute_dml_ddl_query(pg_connection_config, query)
    # return


def main():
    try:
        spark = None
        TASK_ID = "airlines_processor"
        print("Starting ETL job...")
        loaded = load_env()
        if not loaded:
            raise EnvironmentError("Failed to load .env file")

        config = Config()
        parser = argparse.ArgumentParser(description="ETL job: S3 JSON.GZ → Postgres")
        parser.add_argument("--s3_uri", required=True, help="s3 uri stored in database")
        parser.add_argument("--job_name", required=True, help="Spark Application Name")
        parser.add_argument("--run_id", required=True)
        parser.add_argument("--file_log_id", required=True)

        print("Loading environment variables...")

        args = parser.parse_args()

        # Get update query for process_run_log table
        print("Preparing to update process_run_log")
        query, values = get_update_query(
            "monitoring",
            "process_run_log",
            {"status": "Running", "task": f"{TASK_ID}"},
            (("id", "=", args.run_id),),
        )

        execute_dml_ddl_query(config.PG_CONNECTION_CONFIG, query, values)
        print("Updated process_run_log...")

        spark = get_spark_session(
            args.job_name,
            config.REGION,
            config.AWS_ACCESS_KEY_ID,
            config.AWS_SECRET_KEY,
        )

        print("Spark session created.")
        s3_path = f"s3a://{config.S3_BUCKET}/{args.s3_uri}"

        print("Reading source data...")
        df = read_s3_data(spark, s3_path)
        print(f"Got source data with count : {df.count()}...")

        print("Creating staging table if not exists.")
        columns = create_staging_table(
            config.PG_CONNECTION_CONFIG,
            "flight_data",
            "airline",
        )
        columns = [col[0] for col in columns]

        print("Created staging table.")

        # Write staging table.
        pg_config = config.SPARK_PG_CONNECTION_CONFIG
        pg_config["dbtable"] = "staging.airline"
        write_staging(pg_config, df)
        print("Wrote data to staging table.")

        print("Performing upsert from staging to main table...")
        perform_upsert_query(
            "airline",
            "staging",
            "airline",
            "flight_data",
            ("ICAO",),
            columns,
            config.PG_CONNECTION_CONFIG,
        )
        print("Upsert completed.")

        return

    except Exception as e:
        print(f"Failed to complete job : {e.__class__} :{e}")
        traceback.print_exc()
    finally:
        print("Stopping spark session...")
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
