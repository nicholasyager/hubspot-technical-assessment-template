WITH

top10s AS (
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY month
                ORDER BY
                    month DESC, parts_returned DESC, total_revenue_lost DESC
            ) AS seq
        FROM (
            SELECT
                supplier_id,
                part_id,
                {{ date_trunc('month', 'order_date') }} as month,
                COUNT(order_id || line_id) AS parts_returned,
                SUM(customer_cost) AS total_revenue_lost
            FROM
                {{ ref('order_line_items') }}
            WHERE
                item_status = 'R'
            GROUP BY
                month,
                supplier_id,
                part_id
        )
    )
    WHERE
        seq <= 10
    ORDER BY
        month DESC,
        seq DESC
)

SELECT
    top10s.*,
    s.s_name,
    s.s_phone,
    p.p_name
FROM
    top10s
LEFT JOIN {{ source('TPCH_SF1', 'supplier') }} AS s
    ON top10s.supplier_id = s.s_suppkey
LEFT JOIN {{ source('TPCH_SF1', 'part') }} AS p
    ON top10s.part_id = p.p_partkey
