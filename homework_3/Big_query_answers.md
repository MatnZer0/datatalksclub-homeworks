
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `terraform-datatalks.nyc_green_taxi.external_green_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = 
    [
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-01.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-02.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-03.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-04.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-05.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-06.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-07.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-08.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-09.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-10.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-11.parquet',
      'gs://mage-zoomcamp-matn0/nyc_green_taxi_2022/green_tripdata_2022-12.parquet'
    ]
);

-- Question 1. What is count of records for the 2022 Green Taxi Data?
SELECT COUNT(*) FROM terraform-datatalks.nyc_green_taxi.external_green_tripdata;
-- Answer: 840402

-- Check green trip data
SELECT * FROM terraform-datatalks.nyc_green_taxi.external_green_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `terraform-datatalks.nyc_green_taxi.nonpartitioned_green_tripdata`
AS SELECT * FROM `terraform-datatalks.nyc_green_taxi.external_green_tripdata`;

SELECT * FROM terraform-datatalks.nyc_green_taxi.nonpartitioned_green_tripdata limit 10;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE `terraform-datatalks.nyc_green_taxi.partitioned_green_tripdata`
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM `terraform-datatalks.nyc_green_taxi.external_green_tripdata`;

-- Check partitioned green trip data
SELECT * FROM terraform-datatalks.nyc_green_taxi.partitioned_green_tripdata limit 10;

CREATE MATERIALIZED VIEW terraform-datatalks.nyc_green_taxi.view_green_tripdata AS 
SELECT * FROM `terraform-datatalks.nyc_green_taxi.nonpartitioned_green_tripdata`;

-- Question 2. What is the estimated amount of data in the tables?
-- Answer: 0MB and 0MB

-- Question 3. How many records have a fare_amount of 0?
SELECT COUNT(fare_amount) FROM terraform-datatalks.nyc_green_taxi.partitioned_green_tripdata
WHERE fare_amount = 0;
-- Answer: 1622

-- Question 4. What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)

-- Answer: Creating a table partitioned by lpep_pickup_atetime and clustered by PUlocationID
CREATE OR REPLACE TABLE terraform-datatalks.nyc_green_taxi.partitioned_clustered_green_tripdata
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID AS (
SELECT * FROM `terraform-datatalks.nyc_green_taxi.nonpartitioned_green_tripdata`);

-- Question 5. What's the size of the tables?

-- QUERY THE NON PARTITIONED TABLE 
-- Answer: Scanning 12.92 McB of data
SELECT PULocationID, COUNT(PULocationID) FROM terraform-datatalks.nyc_green_taxi.nonpartitioned_green_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
GROUP BY PULocationID;

-- QUERY THE PARTITIONED TABLE 
-- Answer: Scanning ~1.12 MB of DATA
SELECT PULocationID, COUNT(PULocationID) FROM terraform-datatalks.nyc_green_taxi.partitioned_clustered_green_tripdata
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30'
GROUP BY PULocationID;