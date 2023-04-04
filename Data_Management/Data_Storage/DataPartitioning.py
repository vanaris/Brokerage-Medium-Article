import json
import datetime


"""
partition_data method takes the year and month from a given timestamp and creates a partitioned key for the object in the S3 bucket.
This approach enables more efficient querying and retrieval of data based on time, which can be beneficial for insurance data analysis.
"""
""" 
Data partitioning and organization: When storing data in Amazon S3 or Amazon Redshift, it's essential to consider partitioning and organizing the data for efficient querying and retrieval.
In S3, you can partition data by using a proper prefix in the object keys. 
In Redshift, you can use the DISTKEY and SORTKEY options when creating tables to optimize data distribution and sorting.
"""




class DataPartitioning:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def partition_data(self, bucket_name, object_key, data, timestamp:datetime.datetime):
        # Extract year and month from the timestamp
        year = timestamp.year
        month = timestamp.month

        # Create a partitioned key based on the year and month
        partitioned_key = f"year={year}/month={month}/{object_key}"

        # Upload the data to S3 using the partitioned key
        self.s3_client.put_object(Bucket=bucket_name, Key=partitioned_key, Body=json.dumps(data))
        
    def partition_data_by_state(self, bucket_name, object_key, data, state):
        partitioned_key = f"state={state}/{object_key}"
        self.s3_client.put_object(Bucket=bucket_name, Key=partitioned_key, Body=json.dumps(data))
        
    def partition_data_by_state(self, bucket_name, object_key, data, state):
        partitioned_key = f"state={state}/{object_key}"
        self.s3_client.put_object(Bucket=bucket_name, Key=partitioned_key, Body=json.dumps(data))
        
    def delete_old_partitions(self, bucket_name, prefix, retention_period):
        # List objects with the given prefix
        objects = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        # Iterate over objects and delete if they are older than the retention period
        for obj in objects['Contents']:
            object_age = (datetime.now() - obj['LastModified']).days
            if object_age > retention_period:
                self.s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])
                
    def move_processed_data_to_archive(self, bucket_name, object_key, archive_prefix):
        """
        This method moves processed data to an archive partition by copying the original object to the archive partition and then deleting the original object.
        """
        # Copy the object to the archive partition
        archive_key = f"{archive_prefix}/{object_key}"
        self.s3_client.copy_object(
            CopySource={"Bucket": bucket_name, "Key": object_key},
            Bucket=bucket_name,
            Key=archive_key
        )
        
        # Delete the original object
        self.s3_client.delete_object(Bucket=bucket_name, Key=object_key)



