import os
import time
import json
import boto3
from prometheus_client import Counter, start_http_server

JOBS_PROCESSED = Counter("worker_jobs_processed_total", "Total jobs processed", ["status"])

AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")

sqs = boto3.client("sqs", region_name=AWS_REGION)

def process_task(task_data):
    # Simulating processing logic (e.g., resizing image, generating report)
    time.sleep(2)
    print(f"Successfully executed task: {task_data['task_name']} (ID: {task_data['job_id']})")

def main():
    start_http_server(8001)  # Expose metrics on port 8001
    print("Worker started. Listening for messages from SQS...")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=10
            )

            messages = response.get("Messages", [])
            for msg in messages:
                body = json.loads(msg["Body"])
                try:
                    process_task(body)
                    JOBS_PROCESSED.labels(status="success").inc()
                    # Delete message after successful processing
                    sqs.delete_message(
                        QueueUrl=SQS_QUEUE_URL,
                        ReceiptHandle=msg["ReceiptHandle"]
                    )
                except Exception as err:
                    print(f"Error processing job: {err}")
                    JOBS_PROCESSED.labels(status="failed").inc()
        except Exception as e:
            print(f"Error polling SQS: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()