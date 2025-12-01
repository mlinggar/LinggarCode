-- Number 1
select
    o_custkey,
    o_orderdate,
    lag(o_orderdate) over(
        partition by o_custkey
        order by o_orderdate
    ) as prevorderdate,
    datediff(day, prevorderdate, o_orderdate) as dayslastorder
from orders
order by 1,2;

-- Number 2

select 
    o_custkey,
    o_orderdate,
    o_orderkey,
    row_number() over (
        partition by o_custkey
        order by o_orderdate desc
    ) as rownumber
from orders
qualify
    rownumber = 1
order by
    1,2;


-- Number 3
select 
    o_custkey,
    o_orderdate,
    o_totalprice,
    sum(o_totalprice) over (
        partition by o_custkey
        order by o_orderdate
    ) as totalsales
from orders
order by
    1,2;



--
select
    max_by(o_orderkey, o_totalprice, 5)
from snowflake_sample_data.tpch_sf1.orders
;