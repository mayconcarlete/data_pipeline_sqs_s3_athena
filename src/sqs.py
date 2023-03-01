import boto3
import json
QUEUE_NAME = "my-fraud-test"

class SQSService:
    def __init__(self) -> None:
        self.sqs_client = boto3.client("sqs")

    def get_data(self, queue_url):
        response = self.sqs_client.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=1,
        WaitTimeSeconds=5
        )
        message = response.get("Messages", None)
        if message is not None:
          return {
              "receiptHandle": message[0].get("ReceiptHandle"),
              "body": json.loads(message[0].get("Body"))
            }
