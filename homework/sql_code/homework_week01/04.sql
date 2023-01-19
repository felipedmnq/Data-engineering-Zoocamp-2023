WITH top_10_dist AS (
    SELECT trip_distance,
        "PULocationID",
        "DOLocationID"
    FROM green_tripdata_2019
    ORDER BY trip_distance DESC
    LIMIT 10
), taxi_zone AS (
    SELECT *
    FROM taxi_zone_lookup
)
SELECT a.trip_distance,
    a."DOLocationID",
    b."Zone"
FROM top_10_dist a
    LEFT JOIN taxi_zone b ON a."DOLocationID" = b."LocationID"
ORDER BY a.trip_distance DESC