SELECT DATE(lpep_pickup_datetime),
    COUNT(0) AS trip_count,
    passenger_count
FROM green_tripdata_2019
WHERE DATE(lpep_pickup_datetime) = '2019-01-01'
    AND passenger_count in (2, 3)
GROUP BY passenger_count,
    DATE(lpep_pickup_datetime)