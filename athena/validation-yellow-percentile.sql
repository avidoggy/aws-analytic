SELECT
    year,
    month,

    MIN(passenger_count) AS min_passenger_count,
    APPROX_PERCENTILE(passenger_count, 0.995) AS p995_passenger_count,
    MAX(passenger_count) AS max_passenger_count,

    MIN(trip_distance) AS min_trip_distance,
    APPROX_PERCENTILE(trip_distance, 0.995) AS p995_trip_distance,
    MAX(trip_distance) AS max_trip_distance,

    MIN(total_amount) AS min_total_amount,
    APPROX_PERCENTILE(total_amount, 0.995) AS p995_total_amount,
    MAX(total_amount) AS max_total_amount,

    MIN(trip_in_seconds) AS min_trip_in_seconds,
    APPROX_PERCENTILE(trip_in_seconds, 0.995) AS p995_trip_in_seconds,
    MAX(trip_in_seconds) AS max_trip_in_seconds
FROM yellow
WHERE (year = '2020' OR year = '2019' OR year = '2018' OR year = '2017' OR year = '2016' OR year = '2015')
GROUP BY year, month
ORDER BY year, month