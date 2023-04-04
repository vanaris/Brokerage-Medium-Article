import boto3


class DataVersioning:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def enable_versioning(self, bucket_name):
        self.s3_client.put_bucket_versioning(Bucket=bucket_name, VersioningConfiguration={'Status': 'Enabled'})