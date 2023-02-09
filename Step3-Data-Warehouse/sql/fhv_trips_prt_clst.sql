-- Create a table partitioned (pickup_datetime) with a cluster (affiliated_base_number)

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


