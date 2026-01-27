# ETL using Kestra

## Step 1: Setup GCP. Create GCS bucket and BigQuery Dataset

## Step 2: Backfill Data for Green Taxi Trips (2020-01 to 2021-07)

## Step 3: Backfill Data for Yellow Taxi Trips (2020-01 to 2021-07)

## Examining data in GCS Bucket

### Query to get number of rows for all yellow trip CSV files

```sql
SELECT COUNT(*) 
FROM `de-zoomcamp-5467.zoomcamp.yellow_tripdata` 
WHERE filename IN (
  "yellow_tripdata_2020-01.csv",
  "yellow_tripdata_2020-02.csv",
  "yellow_tripdata_2020-03.csv",
  "yellow_tripdata_2020-04.csv",
  "yellow_tripdata_2020-05.csv",
  "yellow_tripdata_2020-06.csv",
  "yellow_tripdata_2020-07.csv",
  "yellow_tripdata_2020-08.csv",
  "yellow_tripdata_2020-09.csv",
  "yellow_tripdata_2020-10.csv",
  "yellow_tripdata_2020-11.csv",
  "yellow_tripdata_2020-12.csv"
)
```

### Query to get number of rows for all green trip CSV files

```sql
SELECT COUNT(*) 
FROM `de-zoomcamp-5467.zoomcamp.green_tripdata` 
WHERE filename IN (
  "green_tripdata_2020-01.csv",
  "green_tripdata_2020-02.csv",
  "green_tripdata_2020-03.csv",
  "green_tripdata_2020-04.csv",
  "green_tripdata_2020-05.csv",
  "green_tripdata_2020-06.csv",
  "green_tripdata_2020-07.csv",
  "green_tripdata_2020-08.csv",
  "green_tripdata_2020-09.csv",
  "green_tripdata_2020-10.csv",
  "green_tripdata_2020-11.csv",
  "green_tripdata_2020-12.csv"
)
```