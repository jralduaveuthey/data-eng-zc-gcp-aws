CREATE OR REPLACE EXTERNAL TABLE `dtc-de-375211.nytaxi.fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://trip_data-dtc-de-zc/fhv_tripdata_2019-*.csv']
);


SELECT count(*) FROM `dtc-de-375211.nytaxi.fhv_tripdata`;


SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `dtc-de-375211.nytaxi.fhv_tripdata`;


CREATE OR REPLACE TABLE `dtc-de-375211.nytaxi.fhv_nonpartitioned_tripdata`
AS SELECT * FROM `dtc-de-375211.nytaxi.fhv_tripdata`;

CREATE OR REPLACE TABLE `dtc-de-375211.nytaxi.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `dtc-de-375211.nytaxi.fhv_tripdata`
);

SELECT count(*) FROM  `dtc-de-375211.nytaxi.fhv_nonpartitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');


SELECT count(*) FROM `dtc-de-375211.nytaxi.fhv_partitioned_tripdata`
WHERE dropoff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
