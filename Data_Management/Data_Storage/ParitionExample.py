# Usage example:
from DataPartitioning import DataPartitioning
import boto3
from datetime import datetime

s3_client = boto3.client('s3')
data_partitioning = DataPartitioning(s3_client)

bucket_name = "your-insurance-data-bucket"
object_key = "customer_data.json"
data = {"customer_id": "123", "name": "John Doe", "policy": "Car Insurance"}

# Assuming the data is collected at this timestamp
timestamp = datetime.now()

data_partitioning.partition_data(bucket_name, object_key, data, timestamp)