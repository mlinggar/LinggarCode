{{ config(materialized='table', schema='silver') }}

with prepared_events as (
    select
        md5(coalesce(route_id, '') || '-' || coalesce(to_varchar(measure_time), '')) as telemetry_event_key,
        md5(route_id) as route_key,
        measure_time as observation_timestamp,
        speed_kmh as current_speed_kmh,
        traffic_status,
        date_trunc('hour', measure_time) as observation_hour,
        ingested_at
    from {{ ref('stg_trafikverket') }}
)

select
    telemetry_event_key,
    route_key,
    observation_timestamp,
    current_speed_kmh,
    traffic_status,
    observation_hour
from prepared_events
qualify row_number() over (partition by telemetry_event_key order by ingested_at desc) = 1