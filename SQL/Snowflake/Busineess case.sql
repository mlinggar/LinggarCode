--- #1: Write a query to calculate monthly regional sales. Use a combination of CTEs, aggregates, date array, etc.

with region_table as (
            select o.o_orderkey,
                   o.o_totalprice,
                   r.r_name
            from orders as o
            inner join customer as c on o.o_custkey = c.c_custkey
            inner join nation as n on c.c_nationkey = n.n_nationkey
            inner join region as r on n.n_nationkey = r.r_regionkey
),

monthly as (
        select o_orderkey, 
               o_totalprice, 
               date_trunc(month, o_orderdate) as month_order
        from orders
)

select m.month_order,
       rt.r_name,
       COUNT(m.o_orderkey) as total_order, 
       SUM(rt.o_totalprice) as sum_total_price
from monthly as m
inner join region_table as rt on m.o_orderkey = rt.o_orderkey
group by m.month_order, rt.r_name
order by m.month_order asc;

--- #2: Write a query to categorize suppliers according to their sales. Use CTEs, aggregates, etc. categorize by revenue

with join_table as (
    select 
        l.l_extendedprice as revenue,
        s.s_name as supplier_name
    from lineitem as l
    inner join supplier as s
        on l.l_suppkey = s.s_suppkey
),

total_sales_supplier as (
    select 
        supplier_name,
        sum(revenue) as total_revenue
    from join_table
    group by supplier_name
)

    select 
        j.supplier_name,
        t.total_revenue,
        case  
            when t.total_revenue < 20000000 then 'poor revenue'
            when t.total_revenue > 25000000 then 'good revenue'
            else 'medium revenue' end 
        as category_revenue     
    from join_table as j
    inner join total_sales_supplier as t
        on j.supplier_name = t.supplier_name
    order by t.total_revenue desc;
    
----

with america as (
    select distinct
        l.l_suppkey as suppkey,
        r.r_name as region_name,
        n.n_name as nation_name,
        c.c_nationkey as nationkey,
    from snowflake_sample_data.tpch_sf1.lineitem as l 
     inner join snowflake_sample_data.tpch_sf1.orders as o
        on l.l_orderkey = o.o_orderkey
    inner join snowflake_sample_data.tpch_sf1.customer as c
        on o.o_custkey = c.c_custkey
    inner join snowflake_sample_data.tpch_sf1.nation as n
        on c.c_nationkey = n.n_nationkey
    inner join snowflake_sample_data.tpch_sf1.region as r
        on n.n_regionkey = r.r_regionkey
    where r.r_name = 'AMERICA'
),

total_revenue as (
    select
        l_suppkey as suppkey,
        round(sum(l_extendedprice),0) as total_revenue,
        iff(suppkey is not null, 'preferred', 'non-preferred') as supplier_type
    from snowflake_sample_data.tpch_sf1.lineitem 
    group by l_suppkey
),

supplier_details as (
    select 
        s.s_name as supplier_name,
        s.s_acctbal as acctbal,
        t.total_revenue as total_revenue,
        t.supplier_type
    from snowflake_sample_data.tpch_sf1.supplier as s
    left outer join total_revenue as t
        on s.s_suppkey = t.suppkey
)

select *
from supplier_details
order by total_revenue desc, acctbal desc
;

with preffered_supplier as (
    select distinct
        l_suppkey as pref_supplier
    from snowflake_sample_data.tpch_sf1.lineitem as l
    inner join snowflake_sample_data.tpch_sf1.orders as o 
        on l.l_orderkey = o.o_orderkey
    inner join snowflake_sample_data.tpch_sf1.customer as c
        on o.o_custkey = c.c_custkey
    inner join snowflake_sample_data.tpch_sf1.nation as n
        on c.c_nationkey = n.n_nationkey
    inner join snowflake_sample_data.tpch_sf1.region as r
        on n.n_regionkey = r.r_regionkey
    where
        r.r_name = 'AMERICA'
),

revenue_by_supplier as (
    select
        l_suppkey,
        sum(l_extendedprice) as revenue
    from snowflake_sample_data.tpch_sf1.lineitem
    group by l_suppkey
),

supplier_summary as (
    select
        l_suppkey as supplier_id,
        revenue as supplier_revenue,
        iff(pref_supplier is not null, 'preferred', 'non-preferred') as supplier_type
    from revenue_by_supplier
    left join preffered_supplier 
        on l_suppkey = pref_supplier
),

supplier_details as (
    select
        s_name,
        s_acctbal,
        supplier_revenue,
        supplier_type
    from snowflake_sample_data.tpch_sf1.supplier
    inner join supplier_summary
        on supplier_id = s_suppkey
)

select *
from supplier_details
order by supplier_revenue desc, s_acctbal desc
;
