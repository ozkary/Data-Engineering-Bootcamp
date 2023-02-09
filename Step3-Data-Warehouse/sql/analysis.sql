-- Data Analysis


-- Select the record count from the materialized table (NOT the external table)

SELECT count(1) FROM trips_data.fhv_trips;

-- Select the distinct count of affiliated_base_number  from the external and materialized table

SELECT COUNT(DISTINCT(affiliated_base_number)) as total 
FROM trips_data.fhv_trips_ext;

SELECT COUNT(DISTINCT(affiliated_base_number)) as total 
FROM trips_data.fhv_trips;

-- How many records have both a blank (null) PUlocationID and DOlocationID 

SELECT COUNT(1) as total FROM trips_data.fhv_trips
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- Compare estimated bytes in queries from a non-partitioned  vs a patitioned/clustered table
- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).

SELECT COUNT(DISTINCT(affiliated_base_number )) as total 
FROM trips_data.fhv_trips
WHERE CAST(pickup_datetime AS DATE) BETWEEN '2019-03-01' AND '2019-03-31'

SELECT COUNT(DISTINCT(affiliated_base_number )) as total 
FROM trips_data.fhv_trips_prt_clst
WHERE CAST(pickup_datetime AS DATE) BETWEEN '2019-03-01' AND '2019-03-31'
