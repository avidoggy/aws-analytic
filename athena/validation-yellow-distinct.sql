SELECT
    CAST(year AS INTEGER) AS year,
    ARRAY_AGG(DISTINCT vendor_name) AS distinct_vendor_name,
    -- ARRAY_AGG(DISTINCT pickup_datetime) AS distinct_pickup_datetime,
    -- ARRAY_AGG(DISTINCT dropoff_datetime) AS distinct_dropoff_datetime,
    -- ARRAY_AGG(DISTINCT passenger_count) AS distinct_passenger_count,
    -- ARRAY_AGG(DISTINCT trip_distance) AS distinct_trip_distance,
    -- ARRAY_AGG(DISTINCT pickup_longitude) AS distinct_pickup_longitude,
    -- ARRAY_AGG(DISTINCT pickup_latitude) AS distinct_pickup_latitude,
    ARRAY_AGG(DISTINCT ratecode_id) AS distinct_ratecode_id,
    ARRAY_AGG(DISTINCT store_and_forward_flag) AS distinct_store_and_forward_flag
    -- ARRAY_AGG(DISTINCT dropoff_longitude) AS distinct_dropoff_longitude,
    -- ARRAY_AGG(DISTINCT dropoff_latitude) AS distinct_dropoff_latitude,
    -- ARRAY_AGG(DISTINCT fare_amount) AS distinct_fare_amount,
    -- ARRAY_AGG(DISTINCT surcharge) AS distinct_surcharge,
    -- ARRAY_AGG(DISTINCT mta_tax) AS distinct_mta_tax,
    -- ARRAY_AGG(DISTINCT tip_amount) AS distinct_tip_amount,
    -- ARRAY_AGG(DISTINCT tolls_amount) AS distinct_tolls_amount,
    -- ARRAY_AGG(DISTINCT total_amount) AS distinct_total_amount,
    -- ARRAY_AGG(DISTINCT payment_type) AS distinct_payment_type,
    -- ARRAY_AGG(DISTINCT pickup_location_id) AS distinct_pickup_location_id,
    -- ARRAY_AGG(DISTINCT dropoff_location_id) AS distinct_dropoff_location_id,
    -- ARRAY_AGG(DISTINCT improvement_surcharge) AS distinct_improvement_surcharge,
    -- ARRAY_AGG(DISTINCT congestion_surcharge) AS distinct_congestion_surcharge,
    -- ARRAY_AGG(DISTINCT trip_in_seconds) AS distinct_trip_in_seconds,
    -- ARRAY_AGG(DISTINCT extra) AS distinct_extra
FROM "nyctlc"."yellow"
GROUP BY year
ORDER BY year