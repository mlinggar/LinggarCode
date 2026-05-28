select
    measure_time,
    route_id,
    route_name,
    linestring_sweref99tm,
    current_speed_kmh,
    current_travel_time_sec,
    free_flow_travel_time_sec,
    traffic_status,
    (current_travel_time_sec - free_flow_travel_time_sec) as delay_seconds
from {{ ref('stg_trafikverket') }}
where measure_time is not null