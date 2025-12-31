import os 
import boto3
from dotenv import load_dotenv

load_dotenv()

def connect_s3():
    s3 = boto3.client(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    return s3