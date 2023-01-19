SELECT DATE(lpep_pickup_datetime),
    trip_distance
FROM green_tripdata_2019
ORDER BY trip_distance DESC
LIMIT 10