import boto3
import pandas as pd
from airflow.models import Variable


def read_s3(file_name):
    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY"),
    )
    s3_client.download_file(
        Bucket="final-project", Key=file_name, Filename=f"/data/{file_name}"
    )

    df = pd.read_csv(f"/data/{file_name}")
    df.drop_duplicates(inplace=True)
    df.to_csv(f"/data/{file_name}", index=False)

    return df
