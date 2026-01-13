from utils.snowflake_conn import snowflake_conn
from utils.logging_config import logger

def get_last_id():
    conn = snowflake_conn()
    cur = conn.cursor()
    
    try:
        cur.execute(f"SELECT MAX(asteroid_id) FROM NEAR_EARTH_OBJECT_DATA")

        max_id = cur.fetchone()[0]
        cur.close()

        if max_id < 1 or max_id is None:
            max_id = 1

        return max_id
    except Exception as e:
        logger.error(f"Erro ao coletar max_id do snowflake: {e}")
