#!/usr/bin/env python3
import argparse
import gzip
import json
import boto3
import io
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit

# spark: SparkSession = (
#     SparkSession.builder.appName("ExampleApp")
#     .master("spark://spark-master:7077")  # Use the container hostname
#     .config("spark.executor.memory", "1G")
#     .config("spark.driver.memory", "1G")
#     .getOrCreate()
# )


def get_spark_session(app_name: str) -> SparkSession:
    """Initialize Spark session with necessary configs."""
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
        .getOrCreate()
    )
    return spark


def download_and_load_json_from_s3(s3_bucket: str, s3_key: str):
    """Download JSON.GZ file from S3 and return as Python list."""
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    with gzip.GzipFile(fileobj=obj["Body"]) as gz:
        data = json.loads(gz.read().decode("utf-8"))
    return dataem


def transform_data(spark, data):
    """Convert list of dicts to Spark DataFrame and transform."""
    df = spark.createDataFrame(data)

    # Example: flatten nested column 'records'
    if "records" in df.columns:
        df = df.withColumn("record", explode(col("records"))).select("record.*")

    # Example: Add load timestamp column
    df = df.withColumn("load_time", lit("current_timestamp()"))

    return df


def upsert_to_postgres(df, pg_url, pg_table, pg_user, pg_password):
    """Write Spark DataFrame to PostgreSQL."""
    (
        df.write.format("jdbc")
        .option("url", pg_url)
        .option("dbtable", pg_table)
        .option("user", pg_user)
        .option("password", pg_password)
        .option("driver", "org.postgresql.Driver")
        .mode("append")  # for incremental loads
        .save()
    )


def main():
    parser = argparse.ArgumentParser(description="ETL job: S3 JSON.GZ → Postgres")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--key", required=True, help="S3 key (path to .json.gz file)")
    parser.add_argument("--pg_url", required=True, help="Postgres JDBC URL")
    parser.add_argument("--pg_table", required=True, help="Target Postgres table name")
    parser.add_argument("--pg_user", required=False, default="postgres")
    parser.add_argument("--pg_password", required=False, default="postgres")
    args = parser.parse_args()

    # Initialize Spark
    spark = get_spark_session("S3ToPostgresETL")

    print(f"Starting ETL job for S3://{args.bucket}/{args.key}")

    # Step 1: Load data
    data = download_and_load_json_from_s3(args.bucket, args.key)

    # Step 2: Transform data
    df = transform_data(spark, data)

    # Step 3: Write to Postgres
    upsert_to_postgres(
        df,
        pg_url=args.pg_url,
        pg_table=args.pg_table,
        pg_user=args.pg_user,
        pg_password=args.pg_password,
    )

    spark.stop()
    print(f"ETL completed successfully for {args.key}")


if __name__ == "__main__":
    main()
