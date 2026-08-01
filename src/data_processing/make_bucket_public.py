import boto3
import json
import os

s3_client = boto3.client('s3',
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id="admin",
    aws_secret_access_key="admin123",
    region_name="us-east-1"
)

bucket_name = "datalake-unstructured"
policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "PublicReadGetObject",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{bucket_name}/*"]
        }
    ]
}

s3_client.put_bucket_policy(Bucket=bucket_name, Policy=json.dumps(policy))
print("Bucket policy set to public read.")
