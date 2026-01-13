import pandas as pd
from utils.logging_config import logger
from datetime import datetime, date, time
from utils.get_max_id import get_last_id

def transform_bronze_to_silver(df):
    try:
        logger.info(f"Tratando os dados da camada bronze para inserir na Silver...")

        df = df[df["name"].notna()].copy()

        df["name"] = df["name"].str.replace("(", "", regex=False).str.replace(")", "", regex=False)
    
        df["absolute_magnitude_h"] = df["absolute_magnitude_h"].astype(float)

        df["hazardous_asteroid"] = df["hazardous_asteroid"].astype(bool)

        df["close_approach_date"] = pd.to_datetime(
            df["close_approach_date"],
            format="%Y-%b-%d %H:%M"
        )

        df["close_approach_date"] = df["close_approach_date"].dt.strftime('%Y-%m-%d %H:%M:%S')

        df["upload_date"] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        print(df.head())
        print(f"Tipo de dado da coluna close_approach_date: {df["close_approach_date"].dtype}")
        print(f"Tipo de dado da coluna upload_date: {df["upload_date"].dtype}")

        #adicionando id
        max_id = get_last_id()
        new_id = max_id + 1

        df["asteroid_id"] = df.range(new_id, new_id + len(df))

        return df
    
    except ValueError as e:
        logger.error(f"Erro ao transformar dados para a camada Silver: {e}")
        raise

    except TypeError as e:
        logger.error(f"Erro ao transformar dados para a camada Silver: {e}")
        raise