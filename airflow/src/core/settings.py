# Todo: Make it proper: Organize this properly, fetch from env do not hardcode.

import os

POSTGRES_DB: str = "flight_db"
POSTGRES_PASSWORD: str = "airflow_password"
POSTGRES_USER: str = "airflow_user"
POSTGRES_HOST: str = "localhost"
POSTGRES_PORT: int = 5432
_schemas = ["flight_data", "monitoring"]

postgres_connection_string = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

aws_secret_key = os.getenv("AWS_SECRET_KEY")
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
s3_bucket = os.getenv("S3_BUCKET")
