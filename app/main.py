import os
import json
import uuid
import boto3
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from prometheus_client import Counter, Histogram, make_asgi_app

app = FastAPI(title="Task Engine API")

# Prometheus Metrics
REQUEST_COUNT = Counter("api_requests_total", "Total API Requests", ["endpoint", "status"])
JOB_SUBMIT_DURATION = Histogram("job_submission_duration_seconds", "Time spent submitting jobs")

# AWS Clients (SQS)
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")

sqs = boto3.client("sqs", region_name=AWS_REGION)

class JobRequest(BaseModel):
    task_name: str
    payload: dict

@app.post("/api/v1/jobs")
def submit_job(job: JobRequest):
    with JOB_SUBMIT_DURATION.time():
        job_id = str(uuid.uuid4())
        message_body = {
            "job_id": job_id,
            "task_name": job.task_name,
            "payload": job.payload
        }
        try:
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            REQUEST_COUNT.labels(endpoint="/api/v1/jobs", status="200").inc()
            return {"status": "QUEUED", "job_id": job_id}
        except Exception as e:
            REQUEST_COUNT.labels(endpoint="/api/v1/jobs", status="500").inc()
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
def health_check():
    return {"status": "healthy"}

# Mount Prometheus metrics route at /metrics
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)