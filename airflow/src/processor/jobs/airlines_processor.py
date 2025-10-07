import argparse
import gzip
import json
import boto3
import io

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit

from src.processor.utils import get_spark_session
from src.core.s3_client import S3Client
from src.core.settings import aws_access_key, aws_secret_key, s3_bucket


def download_and_load_json(client: S3Client, bucket, key):
    obj = client.get_object(bucket, key)
    with gzip.GzipFile(fileobj=obj["body"]) as gz:
        data = json.loads(gz.read().decode("utf-8"))
    return data


def transform_data(spark, data):
    return


def main():
    parser = argparse.ArgumentParser(description="ETL job: S3 JSON.GZ → Postgres")
    parser.add_argument("--s3_uri", required=True, help="s3 uri stored in database")

    args = parser.parse_args()

    s3_uri = args.s3_uri

    spark: SparkSession = get_spark_session("airline_etl")

    s3_client = S3Client(
        bucket=s3_bucket,
        access_key=aws_access_key,
        secret_key=aws_secret_key,
    )

    data = download_and_load_json(s3_client, bucket, key)

    return
