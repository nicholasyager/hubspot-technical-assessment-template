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
        o.customer_id,
        sum(l.l_extendedprice * (1 - l.l_discount)) AS revenue
    FROM {{ source('TPCH_SF1', 'lineitem') }} AS l
    LEFT JOIN orders as o ON
            l.l_orderkey = o.order_id
    WHERE l.l_returnflag = 'R'
    GROUP BY o.customer_id, l.l_orderkey
),

-- Only customers who have orders
ro AS (
    SELECT
        orders.order_id,
        customers.customer_id
    FROM orders
    INNER JOIN customers
        ON orders.customer_id = customers.customer_id
),

rl AS (
    SELECT
        customers.customer_id,
        customers.customer_name,
        customers.account_balance,
        nations.nation_name,
        customers.address,
        customers.phone,
        customers.customer_comment,
        sum(lo.revenue) AS revenue_lost
    FROM ro
    LEFT JOIN lo ON
            lo.order_id = ro.order_id
            AND lo.customer_id = ro.customer_id
    LEFT JOIN customers
        ON customers.customer_id = lo.customer_id
    LEFT JOIN nations ON customers.nation_id = nations.nation_id
    WHERE lo.customer_id IS NOT NULL
    GROUP BY
        customers.customer_id,
        customers.customer_name,
        customers.account_balance,
        customers.phone,
        nations.nation_name,
        customers.address,
        customers.customer_comment
    ORDER BY revenue_lost DESC
)

SELECT * FROM rl
