
"""
create_kms_key: Creates a new KMS key with the specified alias and description.
encrypt_data: Encrypts the specified data using the provided KMS key ID and stores it in the specified S3 object.
decrypt_data: Decrypts the specified S3 object and returns the decrypted data.
re_encrypt_data: Re-encrypts the specified S3 object with a new KMS key ID.
"""



class DataEncryption:
    def __init__(self, s3_client, kms_client):
        self.s3_client = s3_client
        self.kms_client = kms_client

    def enable_encryption(self, bucket_name, encryption_type='SSE-S3'):
        if encryption_type == 'SSE-S3':
            encryption_config = {'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'AES256'}}]}
        elif encryption_type == 'SSE-KMS':
            # Replace 'your-kms-key-id' with your actual KMS key ID
            encryption_config = {'Rules': [{'ApplyServerSideEncryptionByDefault': {'SSEAlgorithm': 'aws:kms', 'KMSMasterKeyID': 'your-kms-key-id'}}]}
        elif encryption_type == 'SSE-C':
            raise NotImplementedError("Client-side encryption is not supported in this example.")
        else:
            raise ValueError("Invalid encryption_type. Use 'SSE-S3', 'SSE-KMS', or 'SSE-C'.")

        self.s3_client.put_bucket_encryption(Bucket=bucket_name, ServerSideEncryptionConfiguration=encryption_config)
        
    def create_kms_key(self, alias_name, key_description):
        response = self.kms_client.create_key(Description=key_description)
        key_id = response['KeyMetadata']['KeyId']
        self.kms_client.create_alias(AliasName=alias_name, TargetKeyId=key_id)
        return key_id
    
    def encrypt_data(self, bucket_name, object_key, data, kms_key_id):
        encrypted_data = self.s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data, ServerSideEncryption='aws:kms', SSEKMSKeyId=kms_key_id)
        return encrypted_data
    
    def decrypt_data(self, bucket_name, object_key):
        response = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)
        return response['Body'].read()
    
    def re_encrypt_data(self, bucket_name, object_key, new_kms_key_id):
        # Retrieve the encrypted data
        encrypted_data = self.decrypt_data(bucket_name, object_key)
        # Re-encrypt the data with the new KMS key
        self.encrypt_data(bucket_name, object_key, encrypted_data, new_kms_key_id)