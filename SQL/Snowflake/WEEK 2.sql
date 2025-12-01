--- Excercise: 1 

SELECT DISTINCT c.c_custkey   
FROM customer AS c
JOIN orders AS o ON c.c_custkey = o.o_custkey;

--- Excercise: 2 
SELECT c.c_custkey, c.c_name, o.o_orderkey
FROM customer AS c
LEFT JOIN orders AS o ON c.C_CUSTKEY = o.o_custkey
WHERE o.o_orderkey IS NULL;

--- Excercise: 3
WITH monthly AS (
    SELECT DATEADD(month, SEQ4(), '2025-01-01') AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT=>10000))
    WHERE month_date <= DATE_TRUNC( month, current_date())
),

nations as (
    SELECT DISTINCT n_name
    FROM NATION
)

SELECT m.month_date,
       n.n_name
FROM monthly AS m
CROSS JOIN nations AS n
ORDER BY m.month_date, n.n_name

--- Excercise 4

WITH monthly AS (
    SELECT DATEADD(month, SEQ4(), '2025-01-01') AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT=>10000))
    WHERE month_date <= DATE_TRUNC( month, current_date())
),

nations as (
    SELECT DISTINCT n_name
    FROM NATION
),

base_table AS (
    SELECT m.month_date,
           na.n_name
    FROM monthly AS m
    CROSS JOIN nations AS na
    ORDER BY m.month_date, na.n_name
),

agg_order AS (
    SELECT n.n_name,
           COUNT(o.o_orderkey) AS total_order,
           DATE_TRUNC(month, o.o_orderdate) AS monthly_order
    FROM orders AS o
    LEFT JOIN customer AS c ON o.o_custkey = c.c_custkey
    LEFT JOIN nation AS n ON c.c_nationkey = n.n_nationkey
    GROUP BY monthly_order, n.n_name
),

join_table AS (
    SELECT bt.n_name,
           ao.total_order,
           ao.monthly_order
    FROM base_table as bt
    LEFT JOIN agg_order as ao ON bt.n_name = ao.n_name
)

SELECT * 
FROM join_table
ORDER BY total_order DESC;

--- Excercise 5
SELECT n.n_name,
       COUNT(o.o_orderkey) AS total_order
FROM orders AS o
INNER JOIN customer AS c ON o.o_custkey = c.c_custkey
INNER JOIN nation AS n on c.c_nationkey = n.n_nationkey AND n.n_name = 'JAPAN'
GROUP BY n.n_name;

--- Excercise 6

SELECT c_name
FROM customer
LIMIT 3;