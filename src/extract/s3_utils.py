import json
import os
import boto3
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_s3_client():
    """Initialize S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "us-east-1")
    )

def get_s3_bucket():
    """Get S3 bucket name from environment"""
    return os.getenv("S3_BUCKET")

def get_partition_path():
    """Generate date partition path"""
    return datetime.utcnow().strftime("%Y-%m-%d")

def upload_json_to_s3(data, dataset_name):
    """
    Upload JSON data to S3 with partitioning
    
    Args:
        data: Dictionary to upload
        dataset_name: Name of dataset (e.g., 'recently_played')
    
    Returns:
        S3 URI of uploaded file
    """
    s3_client = get_s3_client()
    bucket = get_s3_bucket()
    partition = get_partition_path()
    
    # S3 path: raw/dataset_name/dt=YYYY-MM-DD/data.json
    s3_key = f"raw/{dataset_name}/dt={partition}/data.json"
    
    # Upload
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json.dumps(data, indent=2),
        ContentType='application/json'
    )
    
    s3_uri = f"s3://{bucket}/{s3_key}"
    print(f"✅ Uploaded to {s3_uri}")
    
    return s3_uri

def download_json_from_s3(dataset_name, partition=None):
    """
    Download JSON data from S3
    
    Args:
        dataset_name: Name of dataset
        partition: Date partition (defaults to today)
    
    Returns:
        Parsed JSON data
    """
    s3_client = get_s3_client()
    bucket = get_s3_bucket()
    
    if partition is None:
        partition = get_partition_path()
    
    s3_key = f"raw/{dataset_name}/dt={partition}/data.json"
    
    response = s3_client.get_object(Bucket=bucket, Key=s3_key)
    data = json.loads(response['Body'].read())
    
    print(f"✅ Downloaded from s3://{bucket}/{s3_key}")
    
    return data