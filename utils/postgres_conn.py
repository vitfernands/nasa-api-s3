import psycopg2
import os
from dotenv import load_dotenv
from utils.logging_config import logger

load_dotenv()

def connect_to_postgresql():
    try:
        conn = psycopg2.connect(
                host="localhost",
                port=5432,          
                dbname=os.getenv("POSTGRES_DATABASE"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Erro ao conectar ao PostgreSQL: {e}")
        raise