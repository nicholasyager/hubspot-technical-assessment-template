WITH

-- Only customers who have orders
ro AS (
    SELECT
        o.o_orderkey,
        c.c_custkey
    FROM {{ source('TPCH_SF1', 'orders') }} AS o
    INNER JOIN {{ source('TPCH_SF1', 'customer') }} AS c
        ON o.o_custkey = c.c_custkey
),

rl AS (
    SELECT
        c.c_custkey,
        c.c_name,
        c_acctbal,
        n.n_name,
        c_address,
        c_phone,
        c_comment,
        sum(revenue) AS revenue_lost
    FROM ro LEFT JOIN (
        SELECT
            l.l_orderkey AS order_id,
            o.o_custkey AS customer_id,
            sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
        FROM {{ source('TPCH_SF1', 'lineitem') }} AS l
        LEFT JOIN sources.orders AS o ON l.l_orderkey = o.o_orderkey
        WHERE l.l_returnflag = 'R' GROUP BY o.o_custkey, l.l_orderkey
    ) AS lo ON lo.order_id = ro.o_orderkey AND lo.customer_id = ro.c_custkey
    LEFT JOIN
        {{ source('TPCH_SF1', 'customer') }} AS c
        ON c.c_custkey = lo.customer_id
    LEFT JOIN sources.nation AS n ON c.c_nationkey = n.n_nationkey
    WHERE lo.customer_id IS NOT NULL
    GROUP BY
        c.c_custkey,
        c.c_name,
        c.c_acctbal,
        c.c_phone,
        n.n_name,
        c.c_address,
        c.c_comment
    ORDER BY revenue_lost DESC
)

SELECT * FROM rl
