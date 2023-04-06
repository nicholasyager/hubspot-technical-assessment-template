WITH

suppliers AS (
    SELECT * FROM {{ ref('stg_tpch__suppliers') }}
),

parts AS (
    SELECT * FROM {{ ref('stg_tpch__parts') }}
),

monthly_returns AS (
    SELECT
        supplier_id,
        part_id,
        {{ date_trunc('month', 'order_date') }} AS month,
        COUNT(order_id || line_id) AS parts_returned,
        SUM(customer_cost) AS total_revenue_lost
    FROM
        {{ ref('int_order_line_items__mapped') }}
    WHERE
        item_status = 'R'
    GROUP BY
        month,
        supplier_id,
        part_id
),

number_monthly_returns AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY month
            ORDER BY
                month DESC, parts_returned DESC, total_revenue_lost DESC
        ) AS seq
    FROM monthly_returns
),

top10s AS (
    SELECT * FROM number_monthly_returns
    WHERE
        seq <= 10
    ORDER BY
        month DESC,
        seq DESC
),

final AS (
    SELECT
        top10s.*,
        suppliers.supplier_name,
        suppliers.phone,
        parts.part_name
    FROM
        top10s
    LEFT JOIN suppliers
        ON top10s.supplier_id = suppliers.supplier_id
    LEFT JOIN parts
        ON top10s.part_id = parts.part_id
)

SELECT * FROM final
