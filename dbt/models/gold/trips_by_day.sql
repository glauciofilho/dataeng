-- Gold: Daily trip statistics
-- Materialized as an Iceberg table via dbt-trino

{{ config(
    materialized='table',
    file_format='parquet',
    table_type='iceberg'
) }}

SELECT
    pickup_date,
    COUNT(*)                              AS total_trips,
    ROUND(AVG(trip_distance), 2)          AS avg_distance_miles,
    ROUND(AVG(fare_amount), 2)            AS avg_fare_usd,
    ROUND(SUM(total_amount), 2)           AS total_revenue_usd,
    ROUND(AVG(trip_duration_min), 2)      AS avg_duration_min,
    ROUND(AVG(tip_pct), 2)               AS avg_tip_pct,
    ROUND(AVG(passenger_count), 2)        AS avg_passengers
FROM {{ ref('stg_nyc_taxi') }}
GROUP BY pickup_date
ORDER BY pickup_date
