-- Silver staging view: exposes the Iceberg silver.nyc_taxi table
-- produced by Spark, adding computed columns useful for analysis.

{{ config(materialized='view') }}

SELECT
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    EXTRACT(hour FROM pickup_datetime)    AS pickup_hour,
    EXTRACT(dow  FROM pickup_datetime)    AS pickup_dow,   -- 0=Sun … 6=Sat
    trip_duration_min,
    trip_distance,
    passenger_count,
    fare_amount,
    tip_amount,
    total_amount,
    ROUND(tip_amount / NULLIF(fare_amount, 0) * 100, 2) AS tip_pct,
    "PULocationID"                        AS pickup_zone_id,
    "DOLocationID"                        AS dropoff_zone_id,
    payment_type
FROM iceberg.silver.nyc_taxi
WHERE pickup_date IS NOT NULL
