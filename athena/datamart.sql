CREATE TABLE datamart WITH (
    external_location= 's3://gavin-data-lake/nyctlc-datamart/',
    format = 'PARQUET', parquet_compression = 'SNAPPY',
    partitioned_by = ARRAY['year', 'month', 'quarter', 'payment_type', 'pickup_location_id', 'dropoff_location_id'],
    bucketed_by = ARRAY['trip_distance', 'trip_in_seconds', 'avg_speed_per_hour', 'total_amount'],
    bucket_count = 10
) AS
SELECT
    CAST(vendor_name AS INT) AS vendor_name,

    DAY_OF_WEEK(pickup_datetime) AS pickup_day_of_week,
    HOUR(pickup_datetime) AS pickup_hour,

    DAY_OF_WEEK(dropoff_datetime) AS dropoff_day_of_week,
    HOUR(dropoff_datetime) AS dropoff_hour,

    passenger_count,

    trip_distance,
    trip_in_seconds,

    trip_distance/ (trip_in_seconds/3600.0) AS avg_speed_per_hour,
    
    -- pickup_longitude
    -- pickup_latitude
    -- dropoff_longitude
    -- dropoff_latitude
    
    ratecode_id,

    total_amount,

    CAST(year AS INT) AS year,
    CAST(month AS INT) AS month,
    QUARTER(pickup_datetime) AS quarter,
    payment_type,
    pickup_location_id,
    dropoff_location_id
FROM yellow
-- WHERE (year = '2020' OR year = '2019' OR year = '2018' OR year = '2017' OR year = '2016' OR year = '2015')
WHERE (year = '2020')
AND (vendor_name = '1' OR vendor_name = '2')
AND (passenger_count < 10)
AND (trip_distance < 25)
AND (trip_in_seconds BETWEEN 0 AND 5000)
AND (ratecode_id BETWEEN 1 AND 6)
AND (payment_type BETWEEN 1 AND 6)
AND (total_amount < 90)
AND store_and_forward_flag IS NOT NULL
ORDER BY year, month, quarter, payment_type, pickup_location_id, dropoff_location_id, ratecode_id, passenger_count, 'trip_distance', 'trip_in_seconds', 'avg_speed_per_hour', 'total_amount'
LIMIT 10
WITH NO DATA