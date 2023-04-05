WITH

orders AS (
    SELECT * FROM {{ ref('stg_tpch__orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_tpch__customers') }}
),

nations AS (
    SELECT * FROM {{ ref('stg_tpch__nations') }}

),

lo AS (
    SELECT
        l.l_orderkey AS order_id,
        o.o_custkey AS customer_id,
        sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
    FROM {{ source('TPCH_SF1', 'lineitem') }} AS l
    LEFT JOIN sources.orders AS o ON l.l_orderkey = o.o_orderkey
    WHERE l.l_returnflag = 'R' GROUP BY o.o_custkey, l.l_orderkey
),

-- Only customers who have orders
ro AS (
    SELECT
        orders.o_orderkey,
        customers.c_custkey
    FROM orders
    INNER JOIN customers
        ON orders.o_custkey = customers.c_custkey
),

rl AS (
    SELECT
        customers.c_custkey,
        customers.c_name,
        customers.c_acctbal,
        nations.n_name,
        customers.c_address,
        customers.c_phone,
        customers.c_comment,
        sum(lo.revenue) AS revenue_lost
    FROM ro
    LEFT JOIN lo
        ON lo.order_id = ro.o_orderkey AND lo.customer_id = ro.c_custkey
    LEFT JOIN customers
        ON customers.c_custkey = lo.customer_id
    LEFT JOIN nations ON customers.c_nationkey = nations.n_nationkey
    WHERE lo.customer_id IS NOT NULL
    GROUP BY
        customers.c_custkey,
        customers.c_name,
        customers.c_acctbal,
        customers.c_phone,
        nations.n_name,
        customers.c_address,
        customers.c_comment
    ORDER BY revenue_lost DESC
)

SELECT * FROM rl
