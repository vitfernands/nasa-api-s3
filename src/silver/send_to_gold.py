import pandas as pd
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv
from utils.logging_config import logger

load_dotenv()

def load_to_postgres(db_conn, df, table_name):
    try:
        engine = create_engine(
            "postgresql+psycopg2://",
            creator=lambda: db_conn  
        )

        df.to_sql(
            name=table_name,
            con=engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )

        logger.info("Carga realizada no postgres")
    except Exception as e:
        logger.error(f"Erro ao carregar dados no postgres: {e}")