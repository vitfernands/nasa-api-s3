import os
from dotenv import load_dotenv
import snowflake.connector
from utils.logging_config import logger

load_dotenv()

def snowflake_conn():
    try:
        conn = snowflake.connector.connect(
            #user=os.getenv("SNOWFLAKE_USER"),
            #password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            #authenticator='externalbrowser',
            authenticator='oauth',
            token=os.getenv("SNOWFLAKE_TOKEN"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA")
        )

        logger.info("Conexão bem estabelecida com o Snowflake")

        return conn
    except Exception as e:
        logger.error(f"Erro ao estabelecer conexao com o Snowflake: {e}")

    
    

