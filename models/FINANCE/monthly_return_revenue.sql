SELECT
    {{ date_trunc('month', 'order_date') }} as month,
    ROUND(SUM(customer_cost), 2) AS return_total,
    COUNT(DISTINCT order_id) AS orders_with_returns,
    COUNT(CONCAT(order_id, line_id)) AS items_returned
FROM {{ ref('order_line_items') }}
WHERE item_status = 'R'
GROUP BY month
ORDER BY month
