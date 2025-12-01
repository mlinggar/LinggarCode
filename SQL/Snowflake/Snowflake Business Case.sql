-- Select distinct customer also joining with Nation Table to retrieve national name column
-- Using inner join to only retrieve customer with national name
with distinct_customers as (
    select distinct 
        c.c_custkey,
        c.c_name,
        c.c_mktsegment,
        n.n_name 
    from
        snowflake_sample_data.tpch_sf1.customer as c
        inner join snowflake_sample_data.tpch_sf1.nation as n 
            on c.c_nationkey = n.n_nationkey
),

-- Inner join distinct_customer cte with Orders and Lineitem table
-- Inner join because want to retrieve all customer that only have order because RFM model is based on customer transaction behaviour

join_table as (
    select
        dc.c_custkey as custkey,
        o.o_orderkey as orderkey,
        dc.c_name as cust_name,
        dc.n_name as country,
        dc.c_mktsegment as mktsegment,
        o.o_orderdate as orderdate,
        l.l_extendedprice as extendedprice,
        l.l_discount as discount,
        o.o_totalprice as totalprice
    from
        distinct_customers as dc
        inner join snowflake_sample_data.tpch_sf1.orders as o 
            on dc.c_custkey = o.o_custkey
        inner join snowflake_sample_data.tpch_sf1.lineitem as l 
            on o.o_orderkey = l.l_orderkey
),

-- Aggregate cte to find total order, revenue and average order value

agg_table as (
    select
        custkey,
        cust_name,
        country,
        mktsegment,
        -- Using distinct to avoid duplicate and round to no decimal
        round(count(distinct orderkey),0) as total_order,
        -- Calculates total of sales by subtracting the discount from the extendedprice for each item and round to no decimal
        round(sum(extendedprice * (1 - discount)),0) as total_revenue,
        -- Calculates average of order value by totalprice and round to no decimal
        round(avg(totalprice),0) as avg_order_value, 
        -- Get the latest order date for each customer
        max(orderdate) as lastorder 
    from
        join_table
    group by
        custkey,
        cust_name,
        country,
        mktsegment
),

-- Calculating the RFM value using ntile(5) to dividing the customer based on 5 different group
-- RFM Score:
-- 1. Recency: 5 is the most recent, 1 is the least recent
-- 2. Frequency: 5 is the most frequent, 1 is the least frequent
-- 3. Monetary: 5 is the highest value, 1 is the lowest value


rfm_value as (
    select
        custkey,
        cust_name,
        country,
        mktsegment,
        lastorder,
        total_order,
        total_revenue,
        avg_order_value,
        -- Calculate Recency with datediff lastorder date with the latest available purchase date (1998-08-02) in the order table
        -- Sort desc so the smallest difference (most recent) gets the highest score (5)
        ntile(5) over(order by datediff(day, lastorder, '1998-08-02') desc) as recency_value, 
        -- Calculate Frequency: Use total_order
        -- Sort asc so the highest order count gets the highest score (5)
        ntile(5) over(order by total_order) as frequency_value,
        -- Calculate Monetary: Use total_revenue
        -- Sort asc so the highest revenue gets the highest score (5)
        ntile(5) over(order by total_revenue) as monetary_value,
        -- Using ntile(3) to divide average order value into 3 groups to create segments
        ntile(3) over(order by avg_order_value) as avgorder_ntile
    from
        agg_table
),

--- Calculating RFM score by concatenation Recency value, Frequency value and Monetary value

score_rfm as (
    select
        custkey,
        cust_name,
        country,
        mktsegment,
        lastorder,
        total_order,
        total_revenue,
        avg_order_value,
        recency_value,
        frequency_value,
        monetary_value,
        -- Using concat function to get RFM score
        concat(recency_value,frequency_value,monetary_value) as rfm_score,
        avgorder_ntile
    from
        rfm_value
),

-- RFM segment based on Recency as the primary key (5=very recent, 1=least recent), then Monetary value (5=highest, 1=lowest), and Finally Frequency (5=very frequent, 1=least frequent)

rfm_segment as (
    select
        custkey,
        cust_name,
        country,
        mktsegment,
        lastorder,
        total_order,
        total_revenue,
        avg_order_value,
        recency_value,
        frequency_value,
        monetary_value,
        rfm_score,
        case
            -- Champions: top marks in Recency, Frequency, and Monetary
            when recency_value = 5 and frequency_value >= 4 and monetary_value >=4 then 'Champions'
            -- Loyal Customers: active and frequent buyer who purchased relatively recently
            when recency_value >= 4 and frequency_value >= 4 and monetary_value >=1 then 'Loyal Customers'
            -- Potential Loyal Customer: purchased recently but not frequently 
            when recency_value >= 4 and frequency_value <= 3  and monetary_value >= 1 then 'Potential Loyal Customer'
            -- Hibernating Customer: has not ordered in a long time (Recency=2)
            when recency_value = 2 and frequency_value >=1 and monetary_value >= 1 then 'Hibernating Customer'
            -- Lost Customer: has not ordered in a very long time (Recency=1)
            when recency_value = 1 and frequency_value <= 5 and monetary_value <= 5 then 'Lost Customer'
            -- Need Attention/ At Risk Customer: The rest of the customers that do not fit above (primarily Recency=3)
            else 'Need Attention/At Risk'   
        end as rfm_segmentation,
        -- create segment for average order value
        case 
            when avgorder_ntile = 3 then 'High Avg Order Value'
            when avgorder_ntile = 2 then 'Medium Avg Order Value'
            else 'Low Avg Order Value'
        end as avgorder_value_segementation
    from
        score_rfm
    order by
        custkey
)

select *
from rfm_segment;


 




    
/*Part 2. Segment Exploration*/

-- 1.Find out how many customers fall into each segment.

select 
    rfm_segmentation,
    count(custkey) as totalcust_segment -- count custkey to get total number of customer per each segment
from
    rfm_segment
group by 
    rfm_segmentation -- because we want to see for each segment
order by 
    totalcust_segment desc; -- to see from the highest total number of customer per segment

-- 2. Calculate the total revenue generated by each segment.

select 
    rfm_segmentation,
    sum(total_revenue) as totalrevenue_segment -- sum total_revenue to get total number of revenue per each segment
from
    rfm_segment
group by 
    rfm_segmentation -- because we want to see for each segment
order by 
    totalrevenue_segment desc; -- to see from the highest number of revenue per segment

-- 3.Identify the top 5 High Value customers by the RFM score
-- a high-value customer is defined by the highest RFM score and highest average_order_value
select
    custkey,
    cust_name,
    country,
    total_order,
    total_revenue,
    avg_order_value,
    rfm_score,
    rfm_segmentation,
    avgorder_value_segementation,
    dense_rank()over( -- using dense_rank to assign a rank to each customer
        order by rfm_score desc, -- rfm_score as first sort (highest rfm score gets rank 1)
        avg_order_value desc) as rank_customer --  as a secondary sort to see customer with high average order value
from
    rfm_segment
qualify
    rank_customer between 1 and 5; -- filtering only from the first and fifth rank customer 

-- 4. Show which nations have the highest number of High Value customers
select 
    country,
    count(custkey) as total_championscust, -- count customer to get total number of customer per each country
    dense_rank() over(order by count(custkey) desc) as country_rank -- using dense_rank to get the rank column with no gap after tie
from
    rfm_segment
where 
    rfm_segmentation = 'Champions'
    and avgorder_value_segementation =  'High Avg Order Value' -- filtering with only customer with label 'Champions' and 'High Order Value' which they are classified as high value customer
group by 
    country -- because we want to see for each segment
order by 
    total_championscust desc; -- to see from the highest total number of customer per country