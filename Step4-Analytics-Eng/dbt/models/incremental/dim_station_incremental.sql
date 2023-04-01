{{
  config(
    materialized='incremental',     
    incremental_strategy='insert_overwrite',    
    table='{{ ref("dim_station") }}'
  )
}}
-- insert into {{ ref("dim_station") }} (station_id, station_name)
select 
    station_id, 
    station_name    
from {{ ref('stg_inc_station') }}


