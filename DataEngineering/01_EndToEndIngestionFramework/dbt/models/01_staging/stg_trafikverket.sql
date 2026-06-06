{{ config(materialized='view', schema='bronze') }}

select
    coalesce(route.value:Id::string, 'Unknown') as route_id,
    coalesce(route.value:Name::string, 'Unnamed Route') as route_name,
    route.value:Speed::float as speed_kmh,
    route.value:FreeFlowTravelTime::float as free_flow_travel_time_sec,
    route.value:TravelTime::float as travel_time_sec,
    coalesce(route.value:TrafficStatus::string, 'Unknown') as traffic_status,
    route.value:MeasureTime::timestamp_ntz as measure_time,
    route.value:Geometry.SWEREF99TM::string as geometry_sweref99tm,
    ingested_at
from {{ source('traffic_raw', 'raw_trafikverket') }},
lateral flatten(input => json_data:RESPONSE.RESULT) res,
lateral flatten(input => res.value:TravelTimeRoute) route