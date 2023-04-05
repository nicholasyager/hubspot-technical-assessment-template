WITH

buyer_costs AS (
    SELECT
        l_orderkey AS order_id,
        l_linenumber AS line_id,
        l_suppkey AS supplier_id,
        l_partkey AS part_id,
        l_returnflag AS item_status,
        ROUND(l_extendedprice * (1 - l_discount), 2) AS item_cost
    FROM {{ source('TPCH_SF1', 'lineitem') }}
)

SELECT
    o.o_custkey AS customer_id,
    o.o_orderdate AS order_date,
    -- Order id + line id create the primary key
    o.o_orderkey AS order_id,
    bc.line_id,
    bc.supplier_id,
    bc.part_id,
    bc.item_status,
    bc.item_cost AS customer_cost
FROM
    buyer_costs AS bc
LEFT JOIN
    {{ source('TPCH_SF1', 'orders') }} AS o ON bc.order_id = o.o_orderkey
ORDER BY
    o.o_custkey,
    o.o_orderdate,
    o.o_orderkey,
    bc.line_id
