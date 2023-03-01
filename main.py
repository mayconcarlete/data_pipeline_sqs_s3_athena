from io import StringIO
import pandas as pd
from datetime import datetime
import os
import boto3

from src.sqs import SQSService

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

def get_csv_as_df_from_s3(path):
    try:
        df = pd.read_csv(
            path,
            storage_options={
            "key": AWS_ACCESS_KEY_ID,
            "secret": AWS_SECRET_ACCESS_KEY,
            "token": AWS_SESSION_TOKEN,
        })
        return df
    except Exception as error:
        if error.__class__.__name__ == "FileNotFoundError":
            return False

def save_df_as_csv_to_s3_bucket(df, path):
   df.to_csv(
    f"s3://{path}",
    index=False,
    storage_options={
        "key": AWS_ACCESS_KEY_ID,
        "secret": AWS_SECRET_ACCESS_KEY,
        "token": AWS_SESSION_TOKEN,
    },
)

if __name__ == "__main__":
    # get all data and sanatize the data
    sqs_service = SQSService()
    QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/038213797816/my-fraud-test"
    data = []
    receipt_handle_list = []
    for i in range(4):
        sqs_data = sqs_service.get_data(QUEUE_URL)
        if(sqs_data is not None):
            data.append(sqs_data.get("body"))
            receipt_handle_list.append(sqs_data.get("receiptHandle"))
    df = pd.DataFrame(data)
    print(data)
    # check if the bucket exists in S3
    date = datetime.utcnow().strftime('%Y-%m-%d')
    path = f'my-fraud-test-study-case/geocomply/{date}/data.csv'
    csv_file = get_csv_as_df_from_s3(path)
    if not csv_file:
        save_df_as_csv_to_s3_bucket(df, path)
    print("esta indo certo")
