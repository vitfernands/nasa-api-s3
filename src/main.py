from datetime import date
from src.extract.extract import get_api_data
from utils.s3_conn import connect_s3
from utils.postgres_conn import connect_to_postgresql
from src.bronze.bronze_data import send_data_to_s3
import os
from dotenv import load_dotenv
from src.silver.extract_bronze_data import extract_from_bronze
from src.silver.extract_silver_data import extract_from_silver
from src.silver.transform_bronze_to_silver import transform_bronze_to_silver
from src.silver.load_silver_file import load_silver
from src.silver.send_to_gold import load_to_postgres

load_dotenv()

api_key = os.getenv("API_KEY")
bucket_name = os.getenv("S3_BUCKET_NAME")
table_name = 'near_earth_objects'

def main():
    today = str(date.today())
    data = get_api_data(today, api_key)
    s3 = connect_s3()
    send_data_to_s3(data, s3, bucket_name, today)
    df = extract_from_bronze(s3, bucket_name, today)
    df = transform_bronze_to_silver(df)
    load_silver(s3, bucket_name, df, today)
    db_conn = connect_to_postgresql()
    silver_df = extract_from_silver(s3, bucket_name, today)
    load_to_postgres(db_conn, silver_df, table_name)

if __name__ == "__main__":
    main()