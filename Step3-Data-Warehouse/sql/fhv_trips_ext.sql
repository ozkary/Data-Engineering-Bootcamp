### Create an external table by linking it to blob storage files

CREATE OR REPLACE EXTERNAL TABLE trips_data.fhv_trips_ext 
OPTIONS (
  format = 'CSV',
  uris = ['gs://ozkary_data_lake_ozkary-de-101/../data/fhv_tripdata/fhv_tripdata_*.csv.gz']  
);