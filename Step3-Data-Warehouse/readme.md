# Execution Plan

In this step, we focus on Data Warehouse operations and some data analysis by following this plan:

## Python Flows - Data Pipeline

Uploading data in .gz format to google Blob storage with the intend to 
create external tables on Big Query.

### Manual Test Runs (Test Plan)

- $ cd Step3-Data-Warehouse/prefect
- $ python etl_web_to_gcs.py --year=2019 --month=1 --prefix=fhv_tripdata --block_name=blk-gcs-name --url https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/


## Data Warehouse Process

### Create an external table by linking it to blob storage

```
CREATE OR REPLACE EXTERNAL TABLE trips_data.fhv_trips_ext
OPTIONS (
  format = 'CSV',
  uris = ['gs://ozkary_data_lake_ozkary-de-101/../data/fhv_tripdata/fhv_tripdata_*.csv.gz']
);
```


### Create a materialized table from the external table

```
CREATE OR REPLACE TABLE trips_data.fhv_trips AS
SELECT    
      int64_field_0,
      dispatching_base_num,
      CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
      CAST(dropOff_datetime AS TIMESTAMP) AS dropOff_datetime,
      PUlocationID,
      DOlocationID,
      SR_Flag,
      Affiliated_base_number,
FROM `trips_data.fhv_trips_all`
WHERE SAFE_CAST(pickup_datetime AS DATETIME) IS NOT NULL
```

### Create a partitioned table using the pickup_datetime column

```
CREATE OR REPLACE TABLE trips_data.fhv_trips_prt
PARTITION BY DATE(pickup_datetime) AS
SELECT 
  int64_field_0,
      dispatching_base_num,
      pickup_datetime,
      dropOff_datetime,
      PUlocationID,
      DOlocationID,
      SR_Flag,
      Affiliated_base_number
FROM trips_data.fhv_trips;
```

### Create a partitioned (pickup_datetime) table with a cluster (affiliated_base_number)

```
CREATE OR REPLACE TABLE trips_data.fhv_trips_prt_clst
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT int64_field_0,
      dispatching_base_num,
      pickup_datetime,
      dropOff_datetime,
      PUlocationID,
      DOlocationID,
      SR_Flag,
      Affiliated_base_number
FROM trips_data.fhv_trips;

```

## Data Analysis

With this process, we take a look at some general information from our data.

### Select the record count from the materialized table (NOT the external table)

```
SELECT count(1) FROM trips_data.fhv_trips;
```

### Select the distinct count of affiliated_base_number  from the external and materialized table

```
SELECT COUNT(DISTINCT(affiliated_base_number)) as total 
FROM trips_data.fhv_trips_ext;

SELECT COUNT(DISTINCT(affiliated_base_number)) as total 
FROM trips_data.fhv_trips;
```
### How many records have both a blank (null) PUlocationID and DOlocationID 

```
SELECT COUNT(1) as total FROM trips_data.fhv_trips
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;
```

### Compare estimated bytes in queries from a non-partitioned  vs a patitioned/clustered table
- Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).

```
SELECT COUNT(DISTINCT(affiliated_base_number )) as total 
FROM trips_data.fhv_trips
WHERE CAST(pickup_datetime AS DATE) BETWEEN '2019-03-01' AND '2019-03-31'

SELECT COUNT(DISTINCT(affiliated_base_number )) as total 
FROM trips_data.fhv_trips_prt_clst
WHERE CAST(pickup_datetime AS DATE) BETWEEN '2019-03-01' AND '2019-03-31'
```