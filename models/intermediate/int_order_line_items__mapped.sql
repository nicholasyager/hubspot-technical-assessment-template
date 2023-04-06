WITH

orders AS (
    SELECT * FROM {{ ref('stg_tpch__orders') }}
),

buyer_costs AS (
    SELECT
        order_id,
        line_number AS line_id,
        supplier_id,
        part_id,
        return_flag AS item_status,
        ROUND(extended_price * (1 - discount), 2) AS item_cost
    FROM {{ ref('stg_tpch__line_items') }}
),

final AS (
    SELECT
        orders.customer_id,
        orders.order_date,
        -- Order id + line id create the primary key
        orders.order_id,
        buyer_costs.line_id,
        buyer_costs.supplier_id,
        buyer_costs.part_id,
        buyer_costs.item_status,
        buyer_costs.item_cost AS customer_cost
    FROM buyer_costs
    LEFT JOIN orders ON buyer_costs.order_id = orders.order_id
    ORDER BY
        orders.customer_id,
        orders.order_date,
        orders.order_id,
        buyer_costs.line_id
)

SELECT * FROM final
