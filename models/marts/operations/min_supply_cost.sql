SELECT
    ps_partkey AS partkey,
    MIN(ps_supplycost) AS min_supply_cost
FROM
    {{ source('TPCH_SF1', 'partsupp') }},
    {{ source('TPCH_SF1', 'supplier') }},
    {{ source('TPCH_SF1', 'nation') }},
    {{ source('TPCH_SF1', 'region') }}
WHERE
    s_suppkey = ps_suppkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'EUROPE'
GROUP BY ps_partkey
