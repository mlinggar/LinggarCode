{{ config(
    materialized='incremental', 
    unique_key='telemetry_event_key', 
    schema='gold'
) }}

select
    t.telemetry_event_key,
    t.route_key,
    coalesce(w.weather_condition_key, md5('Unknown')) as weather_condition_key,
    t.observation_timestamp,
    t.current_speed_kmh,
    t.traffic_status,
    w.temperature_celsius
from {{ ref('int_traffic_events_cleansed') }} t
left join {{ ref('int_weather_cleansed') }} w
    on t.observation_hour = w.observation_hour

{% if is_incremental() %}
    -- Strictly greater than to prevent identical timestamp overlaps
    where t.observation_timestamp > (select coalesce(max(observation_timestamp), '1970-01-01'::timestamp_ntz) from {{ this }})
{% endif %}

-- Absolute final guarantee: 1 row per event key
qualify row_number() over (partition by t.telemetry_event_key order by t.observation_timestamp desc) = 1