# DEZoomcamp2024
Data engineering 2024 
Solution For Homework module 3

CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp-2024.ny_taxi.external_green_tripdata`
OPTIONS (
  format='parquet',
  uris=['gs://mage-zoomcamp-week2-izzah/nyc_taxi_data_green/2cab630e3e8d44ba84c1a1bc74911b3e-0.parquet']
);

Select count(*) from `dezoomcamp-2024.ny_taxi.external_green_tripdata`;

CREATE OR REPLACE  TABLE `dezoomcamp-2024.ny_taxi.table_green_tripdata_non_partioned` AS
SELECT * FROM `dezoomcamp-2024.ny_taxi.external_green_tripdata`;


Select count(distinct PULocationID) from `dezoomcamp-2024.ny_taxi.external_green_tripdata`;
Select count(distinct PULocationID) from `dezoomcamp-2024.ny_taxi.table_green_tripdata_non_partioned`;

select count(*) from `dezoomcamp-2024.ny_taxi.table_green_tripdata_non_partioned`
where fare_amount=0;

CREATE OR REPLACE  TABLE `dezoomcamp-2024.ny_taxi.table_green_tripdata_partioned` 
partition by DATE(lpep_pickup_datetime)
AS
SELECT * FROM `dezoomcamp-2024.ny_taxi.external_green_tripdata`;

CREATE OR REPLACE  TABLE `dezoomcamp-2024.ny_taxi.table_green_tripdata_partioned_clustered` 
partition by DATE(lpep_pickup_datetime)
CLUSTER BY PULocationID
AS
SELECT * FROM `dezoomcamp-2024.ny_taxi.external_green_tripdata`;



 Select count(distinct PULocationID) from `dezoomcamp-2024.ny_taxi.table_green_tripdata_non_partioned`
 where DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
 Select count(distinct PULocationID) from `dezoomcamp-2024.ny_taxi.table_green_tripdata_partioned_clustered`
 where DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
