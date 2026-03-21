import pandas as pd
import io
import pyarrow.parquet as pq
from utils.logging_config import logger
from botocore.exceptions import ClientError

def extract_from_silver(s3, bucket_name, today):
    try:
        s3_key = f'silver/asteroids/date={today}/asteroids.parquet'

        logger.info("Coletando dados da camada bronze: s3://{bucket_name}/{s3_key}")

        obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
        data = obj["Body"].read()

        buffer = io.BytesIO(data)
        df = pd.read_parquet(buffer)

        return df
    except ClientError as e:
        logger.error(f"Erro ao extrair os dados da camada Silver: {e}")
        raise
