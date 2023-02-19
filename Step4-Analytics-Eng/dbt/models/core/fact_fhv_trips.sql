{{ config(materialized='table',
    partition_by={
      "field": "pickup_datetime",
      "data_type": "timestamp",
      "granularity": "day"},
      cluster_by = "affiliated_base_number",
) }}

with fhv_data as (
    select *        
    from {{ ref('stg_fhv_tripdata') }}
)
select 
    tripid,
    ref_id,
    dispatching_base_num,
    affiliated_base_number,    

     -- timestamps
    pickup_datetime,
    dropoff_datetime,

     -- locations
    pickup_locationid,
    dropoff_locationid,
       
    -- trip info
    share_ride,
    payment_type,
    payment_type_description
from fhv_data