"""
In the DataAccessControl class, uses both Amazon S3 and AWS Identity and Access Management (IAM) to manage access control. The class includes the following methods:

create_bucket_policy: Creates an S3 bucket policy to control access to the specified bucket.
create_iam_role: Creates an IAM role with the specified trust policy.
attach_role_policy: Attaches a policy to an IAM role.
create_iam_user: Creates an IAM user.
attach_user_policy: Attaches a policy to an IAM user.
These methods provide a way to manage access control for both the data in the S3 bucket and the AWS resources required for processing the data. 
"""


import boto3
from botocore.exceptions import ClientError
import json 

class DataAccessControl:
    def __init__(self, s3_client, iam_client):
        self.s3_client = s3_client
        self.iam_client = iam_client

    def create_bucket_policy(self, bucket_name, policy):
        policy_json = json.dumps(policy)
        self.s3_client.put_bucket_policy(Bucket=bucket_name, Policy=policy_json)

    def create_iam_role(self, role_name, trust_policy):
        try:
            response = self.iam_client.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust_policy))
            return response['Role']
        except ClientError as e:
            print(f"Error creating IAM Role: {e}")
            return None

    def attach_role_policy(self, role_name, policy_arn):
        self.iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)

    def create_iam_user(self, user_name):
        try:
            response = self.iam_client.create_user(UserName=user_name)
            return response['User']
        except ClientError as e:
            print(f"Error creating IAM User: {e}")
            return None

    def attach_user_policy(self, user_name, policy_arn):
        self.iam_client.attach_user_policy(UserName=user_name, PolicyArn=policy_arn)
