{{ config(materialized='view') }}

with turnstile as (
    select distinct        
           station        
    from {{ ref('stg_turnstile') }}
    where station is not null
), 

dim_station as (
    select station_id, station_name from {{ ref('dim_station') }}   
)
select 
 {{ dbt_utils.generate_surrogate_key(['station']) }} as station_id,    
  t.station as station_name
from turnstile t
left join dim_station s
   on t.station = s.station_name
where s.station_id is null

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}