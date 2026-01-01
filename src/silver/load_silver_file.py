from io import BytesIO

def load_silver(s3, bucket_name, df, today):
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)

    s3_key = f'silver/asteroids/date={today}/asteroids.parquet'
    
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket_name,
        Key=s3_key,
        Body=buffer.getvalue()
    )

    
        