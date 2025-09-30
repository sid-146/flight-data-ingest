import io
import os
import gzip
import json
from typing import ByteString, List

import boto3


class S3Client:
    def __init__(
        self,
        bucket,
        access_key: str,
        secret_key: str,
        region: str = "ap-south-1",
    ):
        self._bucket = bucket
        self.s3_client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

    @property
    def bucket(self):
        return self._bucket

    @bucket.setter
    def bucket(self, value):
        self._bucket = value

    def create_bucket(self, bucket_name, region):
        raise NotImplementedError(
            f"Function {self.create_bucket.__name__} not implemented. "
        )
        self.s3_client.create_bucket()

    def list_buckets(self) -> None:
        buckets = self.s3_client.buckets.all()
        print(*[bucket.name for bucket in buckets], sep="\n\t")

    def put_compressed_object(self, data:List[dict], bucket_name, put_path, ratio: int = 7):
        s3_key = os.path.join(put_path, "data.json.gz")

        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=ratio) as f:
            f.write(json.dumps(data, indent=2).encode("utf-8"))

        buffer.seek(0)
        self.s3_client.upload_fileobj(buffer, bucket_name, s3_key)

        return f"s3://{bucket_name}/{s3_key}"
