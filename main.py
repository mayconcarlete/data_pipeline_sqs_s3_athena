from io import StringIO
import pandas as pd
from datetime import datetime
import os
import boto3

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")

def get_data_from_sqs():
    csv = pd.read_csv("test.csv")
    csv.to_csv("s3://maycon-fraud/geocomply/data.csv", index=False, storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "token": AWS_SESSION_TOKEN,
    })
    return csv

def get_csv_from_s3(date):
    try:
        df = pd.read_csv(
            f's3://maycon-fraud/geocomply/{date}/datas.csv',
            storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY,
            "token": AWS_SESSION_TOKEN,
        })
        return df
    except Exception as error:
        if error.__class__.__name__ == "FileNotFoundError":
            return False

def save_df_to_s3_bucket(df, date):
    pass

if __name__ == "__main__":
    date = datetime.utcnow().strftime('%Y-%m-%d')
    sqs_data = get_csv_from_s3(date)
    print(sqs_data)
