"""
DAG 02 — Processing
Submits PySpark jobs to process NYC Taxi data:
  bronze/ (raw parquet) → silver/ (Iceberg, cleaned)
  silver/ → gold/ (Iceberg, aggregated)
"""
from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import os

ICEBERG_JAR = "/opt/bitnami/spark/jars/iceberg-spark-runtime-3.5_2.12-1.5.2.jar"

S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:8333")
S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "your_access_key")
S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "your_secret_key")
S3_REGION = os.environ.get("S3_REGION", "us-east-1")
S3_WAREHOUSE_PATH = os.environ.get("S3_WAREHOUSE_PATH", "s3a://warehouse/")
S3_PATH_STYLE_ACCESS = os.environ.get("S3_PATH_STYLE_ACCESS", "true")
ICEBERG_REST_URL = os.environ.get("ICEBERG_REST_URL", "http://iceberg-rest:8181")

SPARK_CONF = {
    # S3A / SeaweedFS S3
    "spark.hadoop.fs.s3a.endpoint":                   S3_ENDPOINT,
    "spark.hadoop.fs.s3a.access.key":                 S3_ACCESS_KEY,
    "spark.hadoop.fs.s3a.secret.key":                 S3_SECRET_KEY,
    "spark.hadoop.fs.s3a.path.style.access":          S3_PATH_STYLE_ACCESS,
    "spark.hadoop.fs.s3a.impl":                       "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":   "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    # Iceberg extensions
    "spark.sql.extensions":                           "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    # Iceberg REST catalog
    "spark.sql.catalog.iceberg":                      "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.iceberg.type":                 "rest",
    "spark.sql.catalog.iceberg.uri":                  ICEBERG_REST_URL,
    "spark.sql.catalog.iceberg.warehouse":            S3_WAREHOUSE_PATH,
    "spark.sql.catalog.iceberg.io-impl":              "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.iceberg.s3.endpoint":          S3_ENDPOINT,
    "spark.sql.catalog.iceberg.s3.path-style-access": S3_PATH_STYLE_ACCESS,
    # Memory
    "spark.executor.memory": "800m",
    "spark.driver.memory":   "512m",
}

with DAG(
    dag_id="02_processing_spark",
    description="Spark: bronze → silver → gold (Iceberg tables)",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["processing", "spark", "iceberg"],
    default_args={"retries": 1, "retry_delay": timedelta(minutes=3)},
) as dag:

    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application="/opt/spark/jobs/bronze_to_silver.py",
        conn_id="spark_default",
        jars=ICEBERG_JAR,
        conf=SPARK_CONF,
        name="bronze_to_silver",
        verbose=False,
    )

    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application="/opt/spark/jobs/silver_to_gold.py",
        conn_id="spark_default",
        jars=ICEBERG_JAR,
        conf=SPARK_CONF,
        name="silver_to_gold",
        verbose=False,
    )

    bronze_to_silver >> silver_to_gold
