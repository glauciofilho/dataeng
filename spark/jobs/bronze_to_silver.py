"""
Job: bronze_to_silver
Reads raw NYC Taxi parquet from MinIO bronze/,
cleans and casts columns, writes as Iceberg table to silver namespace.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

MINIO_ENDPOINT   = "http://minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
ICEBERG_REST_URL = "http://iceberg-rest:8181"

spark = (
    SparkSession.builder
    .appName("bronze_to_silver")
    # ── S3A / MinIO ─────────────────────────────────────────────
    .config("spark.hadoop.fs.s3a.endpoint",                 MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",               MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key",               MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access",        "true")
    .config("spark.hadoop.fs.s3a.impl",                     "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    # ── Iceberg extensions ───────────────────────────────────────
    .config("spark.sql.extensions",                         "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # ── Iceberg REST catalog ─────────────────────────────────────
    .config("spark.sql.catalog.iceberg",                    "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type",               "rest")
    .config("spark.sql.catalog.iceberg.uri",                ICEBERG_REST_URL)
    .config("spark.sql.catalog.iceberg.warehouse",          "s3a://warehouse/")
    .config("spark.sql.catalog.iceberg.io-impl",            "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint",        MINIO_ENDPOINT)
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── 1. Read raw parquet from bronze ─────────────────────────────
print("Reading bronze layer...")
raw = spark.read.parquet("s3a://bronze/nyc_taxi/yellow_tripdata_2023-01.parquet")

# ── 2. Clean & cast ──────────────────────────────────────────────
print("Cleaning data...")
silver = (
    raw
    .withColumnRenamed("tpep_pickup_datetime",  "pickup_datetime")
    .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")
    .withColumn("pickup_datetime",  F.to_timestamp("pickup_datetime"))
    .withColumn("dropoff_datetime", F.to_timestamp("dropoff_datetime"))
    .withColumn("trip_duration_min",
                F.round((F.unix_timestamp("dropoff_datetime") -
                         F.unix_timestamp("pickup_datetime")) / 60, 2))
    .withColumn("fare_amount",       F.col("fare_amount").cast(DoubleType()))
    .withColumn("total_amount",      F.col("total_amount").cast(DoubleType()))
    .withColumn("trip_distance",     F.col("trip_distance").cast(DoubleType()))
    .withColumn("passenger_count",   F.col("passenger_count").cast(IntegerType()))
    .withColumn("pickup_date",       F.to_date("pickup_datetime"))
    # Filter out clearly invalid records
    .filter(F.col("fare_amount") > 0)
    .filter(F.col("trip_distance") > 0)
    .filter(F.col("trip_duration_min") > 0)
    .filter(F.col("trip_duration_min") < 300)  # < 5 hours
    .select(
        "pickup_datetime", "dropoff_datetime", "pickup_date",
        "trip_duration_min", "trip_distance", "passenger_count",
        "fare_amount", "tip_amount", "total_amount",
        "PULocationID", "DOLocationID", "payment_type",
    )
)

# ── 3. Write to Iceberg silver ───────────────────────────────────
print("Writing to Iceberg silver.nyc_taxi...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.silver")

silver.writeTo("iceberg.silver.nyc_taxi") \
      .tableProperty("format-version", "2") \
      .partitionedBy(F.days("pickup_datetime")) \
      .createOrReplace()

count = spark.table("iceberg.silver.nyc_taxi").count()
print(f"Silver table written: {count:,} rows.")

spark.stop()
