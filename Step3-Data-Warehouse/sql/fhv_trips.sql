-- Create a materialized table from the external table

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
FROM trips_data.fhv_trips_ext;
WHERE SAFE_CAST(pickup_datetime AS DATETIME) IS NOT NULL