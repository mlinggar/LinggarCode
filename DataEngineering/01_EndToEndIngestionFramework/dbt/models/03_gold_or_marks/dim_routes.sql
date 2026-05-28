select distinct
    md5(route_id) as route_key,
    route_id,
    route_name,
    linestring_sweref99tm as route_geometry_wkt
from {{ ref('int_route_status') }}