{{ config(materialized='incremental', unique_key='telemetry_event_key', schema='gold') }}

with traffic as ( 
    select * from {{ ref('stg_trafikverket') }} 
    qualify row_number() over (partition by route_id, measure_time order by current_travel_time_sec desc) = 1
),
weather as ( 
    select * from {{ ref('stg_weather') }} 
    qualify row_number() over (partition by date_trunc('hour', ingested_at_cet) order by ingested_at_cet desc) = 1
)

select
    md5(concat(t.route_id, t.measure_time::string)) as telemetry_event_key,
    md5(t.route_id) as route_key,
    
    -- Safe weather condition hash fallback matched with our new dim_weather fallback row
    md5(coalesce(w.weather_condition, 'Unknown')) as weather_condition_key,
    
    t.measure_time as observation_timestamp,
    t.current_speed_kmh,
    t.delay_seconds,
    t.traffic_status,
    w.temperature_celsius

from traffic t
left join weather w 
    on date_trunc('hour', t.measure_time) = date_trunc('hour', w.ingested_at_cet)

{% if is_incremental() %}
  where t.measure_time > (select max(observation_timestamp) from {{ this }})
{% endif %}