import boto3


class S3Client:
    def __init__(self, region: str = "ap-south-1"):
        self.s3_resource = boto3.resource("s3", region)

    def create_bucket(self, bucket_nam, region: str = "ap-south-1"):
        self.s3_resource.create_bucket()

    def list_buckets(self):
        buckets = self.s3_resource.buckets.all()
        print(*[bucket.name for bucket in buckets], sep="\n\t")
