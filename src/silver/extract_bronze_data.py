import pandas as pd
from utils.logging_config import logger
import json
from botocore.exceptions import ClientError

def extract_from_bronze(s3, bucket_name, today):
    try:
        s3_key = f'bronze/asteroids/date={today}/asteroids.json'
        
        logger.info(f"Coletando dados da camada bronze: s3://{bucket_name}/{s3_key}")

        obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
        data = obj["Body"].read()

        json_data = json.loads(data)

        df = pd.json_normalize(json_data)

        #print(df["close_approach_date"].dtype)

        print("--------------------------------------------------")
        print(f"Dados Bronze: \n{df}")
        print("--------------------------------------------------")

        return df
    except ClientError as e:
        logger.error(f"Erro ao coletar dados da camada bronze: {e}")
