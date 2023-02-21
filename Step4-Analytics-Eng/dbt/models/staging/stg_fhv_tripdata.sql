{{ config(materialized='view') }}

with tripdata as 
(
  select *,
    1 as payment_type,
    row_number() over(partition by Affiliated_base_number, pickup_datetime) as rn
  from {{ source('staging','fhv_rides') }}
)
select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['Affiliated_base_number', 'pickup_datetime']) }} as tripid,
    int64_field_0 as ref_id,
    dispatching_base_num,
    Affiliated_base_number as affiliated_base_number,    

     -- timestamps
    pickup_datetime,
    dropoff_datetime,

     -- locations
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
       
    -- trip info
    SR_Flag as share_ride,
    payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description
from tripdata
-- where rn = 1


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}