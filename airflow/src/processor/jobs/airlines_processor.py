import argparse
import os
import logging

from pyspark.sql import SparkSession


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_env():
    from dotenv import load_dotenv

    loaded = load_dotenv()
    logger.info(f"Env loaded : {loaded}")
    return loaded


def main():
    try:
        spark = None
        logger.info("Starting ETL job...")
        loaded = load_env()
        if not loaded:
            raise EnvironmentError("Failed to load .env file")
        parser = argparse.ArgumentParser(description="ETL job: S3 JSON.GZ → Postgres")
        parser.add_argument("--s3_uri", required=True, help="s3 uri stored in database")
        parser.add_argument("--job_name", required=True, help="Spark Application Name")

        HOST = "postgres"  # container name
        PORT = 5432
        USER = os.getenv("POSTGRES_USER")
        PASSWORD = os.getenv("POSTGRES_PASSWORD")
        DB = os.getenv("POSTGRES_DB")
        aws_secret_key = os.getenv("AWS_SECRET_KEY")
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        s3_bucket = os.getenv("S3_BUCKET")
        region = os.getenv("REGION")

        logger.info(f"{aws_secret_key} {aws_access_key} {s3_bucket}")
        # return

        args = parser.parse_args()

        appName = args.job_name
        s3_uri = args.s3_uri
        PG_JDBC_URL = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"

        logger.info(PG_JDBC_URL)

        s3_path = f"s3a://{s3_bucket}/{s3_uri}"
        spark: SparkSession = (
            SparkSession.builder.appName(appName)
            # .config("spark.jars.packages", ",".join(SPARK_PACKAGES))
            .config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key)
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
            .config("spark.hadoop.fs.s3a.path.style.access", "false")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.session.timeZone", "UTC")
            .getOrCreate()
        )

        logger.info("Spark Session Created Successfully.")

        PG_CONNECTION_PROPERTIES = {
            "user": USER,
            "password": PASSWORD,
            "driver": "org.postgresql.Driver",
        }

        logger.info(PG_CONNECTION_PROPERTIES)

        logger.info(f"Reading source data from {s3_path}...")
        source_df = spark.read.option("multiline", "true").json(s3_path)
        logger.info(source_df.schema)
        logger.info("Reading source data from S3...Done.")

        logger.info(f"Records : {source_df.count()}")

        return

    except Exception as e:
        logger.error(f"Failed to complete job : {e.__class__} :{e}")

        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
