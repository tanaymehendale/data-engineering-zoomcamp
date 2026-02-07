# HW3 Queries

## Creating external table in GCS

1. Used [this script](/module-3/bigquery/homework/load_yellow_taxi_data.py) to load into GCS

2. Query to create exteral table - 

```sql
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-5467.bigquery_mod3.external_yellow_tripdata_homework`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-5467-mod3-bucket/yellow_tripdata_2024-*.parquet']
);
```

## Create materialized view

```sql
CREATE OR REPLACE TABLE `de-zoomcamp-5467.bigquery_mod3.yellow_jan_to_june_2024`
AS SELECT * FROM `de-zoomcamp-5467.bigquery_mod3.external_yellow_tripdata_homework`;
```

## Questions

### Counting records 

What is count of records for the 2024 Yellow Taxi Data?

```sql
SELECT COUNT(*) FROM `de-zoomcamp-5467.bigquery_mod3.yellow_jan_to_june_2024`
```
 - 20,332,093

### Data Read Estimation

Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.

```sql
-- External Table
SELECT COUNT(DISTINCT PULocationID) FROM `de-zoomcamp-5467.bigquery_mod3.external_yellow_tripdata_homework`

-- Materialized Table
SELECT COUNT(DISTINCT PULocationID) FROM `de-zoomcamp-5467.bigquery_mod3.yellow_jan_to_june_2024`
```

What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

 - 0 MB for the External Table and 155.12 MB for the Materialized Table

### Understanding Columnar Storage

Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. Now write a query to retrieve the PULocationID and DOLocationID on the same table.
Why are the estimated number of Bytes different?

 - BigQuery is a columnar database, and it only scans the specific columns requested in the query. Querying two columns (PULocationID, DOLocationID) requires reading more data than querying one column (PULocationID), leading to a higher estimated number of bytes processed.


### Counting zero fare trips

How many records have a fare_amount of 0?

```sql
SELECT COUNT(*) FROM `de-zoomcamp-5467.bigquery_mod3.yellow_jan_to_june_2024` WHERE fare_amount = 0
```

 - 8333


### Partitioning and clustering

What is the best strategy to make an optimized table in Big Query if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)

 - Partition by tpep_dropoff_datetime and Cluster on VendorID

```sql
CREATE OR REPLACE TABLE `de-zoomcamp-5467.bigquery_mod3.optimized_table_yellow_2024`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `de-zoomcamp-5467.bigquery_mod3.external_yellow_tripdata_homework`
);
```

### Partition benefits

Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

```sql
-- Using Materialized table
SELECT COUNT(DISTINCT VendorID) FROM `de-zoomcamp-5467.bigquery_mod3.yellow_jan_to_june_2024` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'

-- Using partitioned table
SELECT COUNT(DISTINCT VendorID) FROM `de-zoomcamp-5467.bigquery_mod3.partitioned_yellow_jan_to_june_2024` WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'
```

 - 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


### External table storage

Where is the data stored in the External Table you created?
 - GCP Bucket

### Clustering best practices

It is best practice in Big Query to always cluster your data:
 - False

### Understanding table scans

Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?

 - It estimates 0B will be processed. This happens because row count is already present in the metadata of the materialized table and BigQuery picks that. This can also happen because we ran the same query in Question 1, and as a result BigQuery cached that result