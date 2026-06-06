{{ config(
    materialized='incremental', 
    unique_key='telemetry_event_key', 
    schema='gold'
) }}

with base_traffic as (
    select
        t.telemetry_event_key,
        t.route_key,
        coalesce(w.weather_condition_key, md5('Unknown')) as weather_condition_key,
        t.observation_timestamp,
        t.current_speed_kmh,
        t.traffic_status,
        w.temperature_celsius,
        t.observation_hour
    from {{ ref('int_traffic_events_cleansed') }} t
    left join {{ ref('int_weather_cleansed') }} w
        on t.observation_hour = w.observation_hour
    
    {% if is_incremental() %}
        where t.observation_timestamp > (select coalesce(max(observation_timestamp), '1970-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}
)

select
    b.telemetry_event_key,
    b.route_key,
    b.weather_condition_key,
    -- CRITICAL STEP: Explicit directly mapped connection link established with dim_osm_assets
    coalesce(o.asset_key, md5('No Asset')) as asset_key,
    b.observation_timestamp,
    b.current_speed_kmh,
    b.traffic_status,
    b.temperature_celsius
from base_traffic b
left join {{ ref('dim_osm_assets') }} o
    on b.route_key = o.route_key

qualify row_number() over (partition by b.telemetry_event_key order by b.observation_timestamp desc) = 1