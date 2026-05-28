with flattened_routes as (
    select 
        ingested_at as ingested_at_cet,
        file_path,
        res.value as route_block
    from {{ source('azure_bronze', 'raw_trafikverket') }},
    lateral flatten(input => json_data:RESPONSE:RESULT[0]:TravelTimeRoute) res
)
select
    ingested_at_cet,
    file_path,
    route_block:Id::string as route_id,
    route_block:Name::string as route_name,
    route_block:MeasureTime::timestamp as measure_time,
    route_block:FreeFlowTravelTime::float as free_flow_travel_time_sec,
    route_block:TravelTime::float as current_travel_time_sec,
    route_block:Speed::float as current_speed_kmh,
    route_block:TrafficStatus::string as traffic_status,
    route_block:Geometry:SWEREF99TM::string as linestring_sweref99tm
from flattened_routes