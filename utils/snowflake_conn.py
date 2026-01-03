import os
from dotenv import load_dotenv
import snowflake.connector
from utils.logging_config import logger

load_dotenv()

def snowflake_conn():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    
def get_last_id() -> int:
    try:
        conn = snowflake_conn()
        cur = conn.cursor()

        cur.execute("SELECT MAX(asteroid_id) FROM NEAR_EARTH_OBJECT_DATA")

        max_id = cur.fetchone()[0]
        cur.close()

        if max_id < 1:
            return 1
        
        return max_id
    except Exception as e:
        logger.error(f"Erro ao coletar max_id da tabela NEAR_EARTH_OBJECT_DATA: {e}")

