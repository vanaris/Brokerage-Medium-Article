import boto3

class DataLifecycleManagement:
    def __init__(self, s3_client):
        self.s3_client = s3_client

    def create_lifecycle_policy(self, bucket_name, policy):
        self.s3_client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=policy)
        
    def list_object_versions(self, bucket_name, object_key):
        """ 
        This method lists all versions of a specific object in the bucket.
        """
        object_versions = self.s3_client.list_object_versions(Bucket=bucket_name, Prefix=object_key)
        return object_versions['Versions']
    
    def get_object_version(self, bucket_name, object_key, version_id):
        """Method to retrieve a specific version of an object """
        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key, VersionId=version_id)
        return response['Body'].read()
    
    def delete_object_version(self, bucket_name, object_key, version_id):
        
        """Method to delete a specific version of an object:
        """
        self.s3_client.delete_object(Bucket=bucket_name, Key=object_key, VersionId=version_id)
        
    def rollback_to_previous_version(self, bucket_name, object_key, version_id):
        """ This method rolls back the object to a previous version using the specified version ID. """
        # Retrieve the specific version of the object
        previous_version_data = self.get_object_version(bucket_name, object_key, version_id)

        # Overwrite the current version of the object with the retrieved version data
        self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=previous_version_data)



    
    