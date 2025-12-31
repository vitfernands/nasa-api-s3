import utils.logging_config as log_config
from botocore.exceptions import ClientError
import json

def send_data_to_s3(data, s3, bucket_name, today):
    try:
        s3_key = f'bronze/asteroids/date={today}/asteroids.json'

        s3.put_object(
            Bucket=bucket_name,  
            Key=s3_key,
            Body=json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
    except ClientError as e:
        log_config.logger.error(f"Erro ao enviar arquivos para o s3: {e}")