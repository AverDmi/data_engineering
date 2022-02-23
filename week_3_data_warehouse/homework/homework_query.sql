CREATE OR REPLACE EXTERNAL TABLE `innate-temple-338718.trips_data_all.external_fhv_data`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_innate-temple-338718/raw/fhv_tripdata_2019-*.parquet']
);

SELECT COUNT(*) FROM innate-temple-338718.trips_data_all.external_fhv_data;

SELECT COUNT(DISTINCT(dispatching_base_num)) FROM innate-temple-338718.trips_data_all.external_fhv_data;

SELECT * FROM innate-temple-338718.trips_data_all.external_fhv_data WHERE SR_Flag IS NOT NULL LIMIT 10;

CREATE OR REPLACE TABLE innate-temple-338718.trips_data_all.external_fhv_data_partitoned_clustered
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS
SELECT * FROM innate-temple-338718.trips_data_all.external_fhv_data;

SELECT COUNT(*) AS trips
FROM innate-temple-338718.trips_data_all.external_fhv_data_partitoned_clustered
WHERE DATE(dropoff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02060', 'B02279');

SELECT COUNT(DISTINCT(dispatching_base_num)) FROM innate-temple-338718.trips_data_all.external_fhv_data;

SELECT COUNT(DISTINCT(SR_Flag)) FROM innate-temple-338718.trips_data_all.external_fhv_data;
