WITH

orders AS (
    SELECT * FROM {{ ref('stg_tpch__orders') }}
),

buyer_costs AS (
    SELECT
        l_orderkey AS order_id,
        l_linenumber AS line_id,
        l_suppkey AS supplier_id,
        l_partkey AS part_id,
        l_returnflag AS item_status,
        ROUND(l_extendedprice * (1 - l_discount), 2) AS item_cost
    FROM {{ ref('stg_tpch__line_items') }}
),

final AS (
    SELECT
        orders.o_custkey AS customer_id,
        orders.o_orderdate AS order_date,
        -- Order id + line id create the primary key
        orders.o_orderkey AS order_id,
        buyer_costs.line_id,
        buyer_costs.supplier_id,
        buyer_costs.part_id,
        buyer_costs.item_status,
        buyer_costs.item_cost AS customer_cost
    FROM buyer_costs
    LEFT JOIN orders ON buyer_costs.order_id = orders.o_orderkey
    ORDER BY
        orders.o_custkey,
        orders.o_orderdate,
        orders.o_orderkey,
        buyer_costs.line_id
)

SELECT * FROM final
