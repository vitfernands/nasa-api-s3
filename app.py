import os
import requests
from dotenv import load_dotenv
import json
from datetime import date
import boto3
from botocore.exceptions import ClientError
import logging

load_dotenv()

#variaveis de ambiente
api_key = os.getenv('API_KEY')
bucket_name = os.getenv("S3_BUCKET_NAME")

#configurando logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

logger = logging.getLogger(__name__)

def get_api_data(today, api_key):
    try:
        logger.info("Chamando API...")

        asteroid_list = []

        r = json.loads(
            requests.get(
                f'https://api.nasa.gov/neo/rest/v1/feed',
                params={
                    'start_date': today,
                    'end_date': today,
                    'api_key': api_key
                },
                timeout=30
            ).text
        )

        element_count = {
            "element_count": r["element_count"]
        }

        asteroid_list.append(element_count)

        #Pegando apenas as informacoes necessarias do retorno da API
        for data, near_earth_objects in r["near_earth_objects"].items():
            for asteroid in near_earth_objects:
                infos = {
                    "name": asteroid["name"],
                    "absolute_magnitude_h": asteroid["absolute_magnitude_h"],
                    "hazardous_asteroid": asteroid["is_potentially_hazardous_asteroid"],
                    "close_approach_date": asteroid["close_approach_data"][0]["close_approach_date_full"]
                }

                asteroid_list.append(infos)

        logger.info("Response da API coletado com sucesso!")

        return asteroid_list
    
    except requests.exceptions.ReadTimeout:
        logger.error("Timeout ao chamar a API")

    except requests.exceptions.HTTPError as e:
        logger.error(f"Erro HTTP: {e}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Erro ao chamar a API: {e}")

def connect_s3():
    s3 = boto3.resource(
        service_name='s3',
        region_name='us-east-2',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY")
    )
    return s3

def send_data_to_s3(data, s3, bucket_name, today):
    try:
        s3_key = f'asteroids/date={today}/asteroids2.json'

        s3.Object(bucket_name, s3_key).put(
            Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
    except ClientError as e:
        logger.error(f"Erro ao enviar arquivos para o s3: {e}")

def main():
    today = str(date.today())
    data = get_api_data(today, api_key)
    s3 = connect_s3()
    send_data_to_s3(data, s3, bucket_name, today)

if __name__ == "__main__":
    main()
