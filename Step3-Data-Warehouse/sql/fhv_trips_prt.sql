-- Create a partitioned table using the pickup_datetime column

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