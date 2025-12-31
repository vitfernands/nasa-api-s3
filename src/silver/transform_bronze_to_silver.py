import pandas as pd
from utils.logging_config import logger
from datetime import datetime, date

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

        df["upload_date"] = datetime.now()

        return df
    
    except ValueError as e:
        logger.error(f"Erro ao transformar dados para a camada Silver: {e}")

    except TypeError as e:
        logger.error(f"Erro ao transformar dados para a camada Silver: {e}")