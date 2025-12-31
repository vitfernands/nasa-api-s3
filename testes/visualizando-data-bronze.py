from datetime import date
from utils.s3 import connect_s3
import os
from dotenv import load_dotenv
from src.silver.extract_bronze_data import extract_from_bronze
from src.silver.transform_bronze_to_silver import transform_bronze_to_silver

load_dotenv()

api_key = os.getenv("API_KEY")
bucket_name = os.getenv("S3_BUCKET_NAME")

def teste():
    today = str(date.today())
    #data = get_api_data(today, api_key)
    s3 = connect_s3()
    #send_data_to_s3(data, s3, bucket_name, today)
    df = extract_from_bronze(s3, bucket_name, today)
    transform_bronze_to_silver(df)

teste()