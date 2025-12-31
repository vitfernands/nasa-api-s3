import os
from dotenv import load_dotenv
import requests
from utils.logging_config import logger
import json

load_dotenv()

#variaveis de ambiente
api_key = os.getenv('API_KEY')

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