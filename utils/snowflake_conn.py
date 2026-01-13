import os
from cryptography.hazmat.primitives import serialization
import snowflake.connector
from utils.logging_config import logger

def snowflake_conn():
    try:
        with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as key:
            private_key = serialization.load_pem_private_key(
                key.read(),
                password=None
            )

        private_key_bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        conn = snowflake.connector.connect(
            user=os.environ["SNOWFLAKE_USER"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            private_key=private_key_bytes,
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
        )

        return conn
    except Exception as e:
        logger.error(f"Erro ao efetuar conexao com o Snowflake: {e}")
    finally:
        logger.info("Conexao com o Snowflake efetuada com sucesso")
    
    

