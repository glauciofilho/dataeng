"""
Job: silver_to_gold
Reads Iceberg silver.nyc_taxi and produces gold-layer aggregated tables:
  - gold.trips_by_day
  - gold.trips_by_zone
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import os

S3_ENDPOINT   = os.getenv("S3_ENDPOINT", "http://127.0.0.1:8333")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY", "your_access_key")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY", "your_secret_key")
S3_WAREHOUSE  = os.getenv("S3_WAREHOUSE_PATH", "s3a://warehouse/")
S3_PATH_STYLE_ACCESS = os.getenv("S3_PATH_STYLE_ACCESS", "true")
ICEBERG_REST_URL = os.getenv("ICEBERG_REST_URL", "http://iceberg-rest:8181")

spark = (
    SparkSession.builder
    .appName("silver_to_gold")
    .config("spark.hadoop.fs.s3a.endpoint",                 S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",               S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",               S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",        S3_PATH_STYLE_ACCESS)
    .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.extensions",                         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg",                    "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type",               "rest")
    .config("spark.sql.catalog.iceberg.uri",                ICEBERG_REST_URL)
    .config("spark.sql.catalog.iceberg.warehouse",          S3_WAREHOUSE)
    .config("spark.sql.catalog.iceberg.io-impl",            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint",        S3_ENDPOINT)
    .config("spark.sql.catalog.iceberg.s3.path-style-access", S3_PATH_STYLE_ACCESS)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

silver = spark.table("iceberg.silver.nyc_taxi")

# ── Gold 1: trips_by_day ─────────────────────────────────────────
print("Building gold.trips_by_day...")
trips_by_day = (
    silver.groupBy("pickup_date")
    .agg(
        F.count("*").alias("total_trips"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue_usd"),
        F.round(F.avg("trip_duration_min"), 2).alias("avg_duration_min"),
        F.round(F.avg("passenger_count"), 2).alias("avg_passengers"),
    )
    .orderBy("pickup_date")
)

trips_by_day.writeTo("iceberg.gold.trips_by_day") \
            .tableProperty("format-version", "2") \
            .createOrReplace()

# ── Gold 2: trips_by_zone ────────────────────────────────────────
print("Building gold.trips_by_zone...")
trips_by_zone = (
    silver.groupBy("PULocationID")
    .agg(
        F.count("*").alias("total_pickups"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare_usd"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue_usd"),
    )
    .withColumnRenamed("PULocationID", "location_id")
    .orderBy(F.desc("total_pickups"))
)

trips_by_zone.writeTo("iceberg.gold.trips_by_zone") \
             .tableProperty("format-version", "2") \
             .createOrReplace()

print("Gold tables written.")
spark.stop()
