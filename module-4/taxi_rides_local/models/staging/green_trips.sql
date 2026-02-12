-- SELECT * FROM taxi_rides_local.prod.green_tripdata
SELECT * 
FROM {{ source('raw_data', 'green_tripdata') }}