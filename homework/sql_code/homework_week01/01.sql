SELECT 
    COUNT(*) 
FROM green_tripdata_2019 
WHERE DATE(lpep_pickup_datetime) = '2019-01-15'