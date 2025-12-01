with customers as (
    select
        c.c_custkey,
        c.c_name,
        c.c_address,
        c.c_phone,
        c.c_acctbal,
        c.c_mktsegment,
        n.n_name
    from snowflake_sample_data.tpch_sf1.customer as c
    inner join snowflake_sample_data.tpch_sf1.nation as n
        on c.c_nationkey = n.n_nationkey
),

agg_table as (
    select
        c.c_custkey as custkey,
        o.o_orderstatus,
        count(distinct o.o_orderkey) as total_order,
        sum(l.l_extendedprice * (1-l.l_discount) * (1+l.l_tax)) as total_price_before_discount,
        avg(l.l_extendedprice * (1-l.l_discount) * (1+l.l_tax)) as avg_price_before_discount,
        min(o.o_orderdate) as first_order,
        max(o.o_orderdate) as last_order,
    from snowflake_sample_data.tpch_sf1.customer as c
    left join snowflake_sample_data.tpch_sf1.orders as o
        on c.c_custkey = o.o_custkey
    left join snowflake_sample_data.tpch_sf1.lineitem as l
        on o.o_orderkey = l.l_orderkey
    group by
        c.c_custkey,
        o_orderstatus
),

segment_table as (
    select
        custkey,
        o_orderstatus,
        total_order,
        total_price_before_discount,
        avg_price_before_discount,
        first_order,
        last_order,
        case
            when total_price_before_discount > 600000 then 'high revenue'
            when total_price_before_discount < 200000 then 'low revenue'
            when total_price_before_discount is null then 'not yet order'
            else 'medium revenue'
        end as revenue_types
    from agg_table
),

cust_segment as (
    select
        cs.c_custkey,
        cs.c_name,
        cs.c_address,
        cs.c_phone,
        cs.c_acctbal,
        cs.c_mktsegment,
        cs.n_name,
        st.o_orderstatus,
        st.total_order,
        round(st.total_price_before_discount,0) as total_price_before_discount,
        round(st.avg_price_before_discount,0) as avg_price_before_discount,
        st.first_order,
        st.last_order,
        st.revenue_types
    from customers as cs
    left join segment_table as st
    on cs.c_custkey = st.custkey
),

brands as (
     select
        ct.c_custkey,
        p.p_brand,
        count(li.l_orderkey) as total_order
    from customers as ct
    inner join snowflake_sample_data.tpch_sf1.orders as od 
        on ct.c_custkey = od.o_custkey
    inner join snowflake_sample_data.tpch_sf1.lineitem as li
        on od.o_orderkey = li.l_orderkey
    inner join snowflake_sample_data.tpch_sf1.part as p
        on li.l_partkey = p.p_partkey
    group by
         ct.c_custkey,
            p.p_brand
    qualify row_number() over (
        partition by ct.c_custkey
        order by count(li.l_orderkey) desc
    ) = 1
),

final as (
    select 
            cm.c_name,
            cm.c_address,
            cm.c_phone,
            cm.c_acctbal,
            cm.c_mktsegment,
            cm.n_name,
            cm.o_orderstatus,
            cm.total_order,
            b.p_brand,
            cm.total_price_before_discount,
            cm.avg_price_before_discount,
            cm.first_order,
            cm.last_order,
            cm.revenue_types
    from cust_segment as cm
    left join brands as b
        on cm.c_custkey = b.c_custkey
)

select *
from final
;

