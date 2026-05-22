"""
DAG 01 — Ingestion
Downloads NYC Yellow Taxi Parquet from public source and uploads to MinIO bronze/.
"""
from __future__ import annotations

import os
import io
import logging
from datetime import datetime, timedelta

import requests
import boto3
from botocore.client import Config
from airflow import DAG
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

MINIO_ENDPOINT     = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY   = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET_KEY   = os.environ["MINIO_ROOT_PASSWORD"]
BRONZE_BUCKET      = "bronze"

# Public NYC Taxi data — Jan 2023 (~45 MB parquet)
NYC_TAXI_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    "yellow_tripdata_2023-01.parquet"
)
DEST_KEY = "nyc_taxi/yellow_tripdata_2023-01.parquet"


def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def download_and_upload(**context):
    """Stream NYC Taxi parquet directly into MinIO bronze bucket."""
    s3 = _s3_client()

    # Check if already uploaded (idempotent)
    try:
        s3.head_object(Bucket=BRONZE_BUCKET, Key=DEST_KEY)
        log.info("File already exists in bronze bucket, skipping download.")
        return
    except s3.exceptions.ClientError:
        pass

    log.info("Downloading NYC Taxi data from %s", NYC_TAXI_URL)
    with requests.get(NYC_TAXI_URL, stream=True, timeout=300) as resp:
        resp.raise_for_status()
        data = resp.content

    log.info("Uploading %d MB to s3://%s/%s", len(data) // 1024 // 1024, BRONZE_BUCKET, DEST_KEY)
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=DEST_KEY,
        Body=data,
        ContentType="application/octet-stream",
    )
    log.info("Upload complete.")


with DAG(
    dag_id="01_ingestion_nyc_taxi",
    description="Download NYC Yellow Taxi parquet → MinIO bronze/",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,          # trigger manually or via full pipeline DAG
    catchup=False,
    tags=["ingestion", "bronze", "nyc-taxi"],
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
) as dag:

    ingest = PythonOperator(
        task_id="download_nyc_taxi_to_bronze",
        python_callable=download_and_upload,
    )
