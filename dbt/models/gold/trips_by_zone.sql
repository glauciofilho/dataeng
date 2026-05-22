-- Gold: Pickup zone performance
-- Top zones by volume, revenue and average metrics

{{ config(
    materialized='table',
    file_format='parquet',
    table_type='iceberg'
) }}

SELECT
    pickup_zone_id,
    COUNT(*)                              AS total_pickups,
    ROUND(AVG(fare_amount), 2)            AS avg_fare_usd,
    ROUND(AVG(trip_distance), 2)          AS avg_distance_miles,
    ROUND(SUM(total_amount), 2)           AS total_revenue_usd,
    ROUND(AVG(tip_pct), 2)               AS avg_tip_pct
FROM {{ ref('stg_nyc_taxi') }}
GROUP BY pickup_zone_id
ORDER BY total_pickups DESC
