from datetime import date
from src.extract.extract import get_api_data
from utils.s3_conn import connect_s3
from src.bronze.bronze_data import send_data_to_s3
import os
from dotenv import load_dotenv
from src.silver.extract_bronze_data import extract_from_bronze
from src.silver.transform_bronze_to_silver import transform_bronze_to_silver
from src.silver.load_silver_file import load_silver

load_dotenv()

api_key = os.getenv("API_KEY")
bucket_name = os.getenv("S3_BUCKET_NAME")

def main():
    today = str(date.today())
    data = get_api_data(today, api_key)
    s3 = connect_s3()
    send_data_to_s3(data, s3, bucket_name, today)
    df = extract_from_bronze(s3, bucket_name, today)
    df = transform_bronze_to_silver(df)
    load_silver(s3, bucket_name, df, today)

if __name__ == "__main__":
    main()