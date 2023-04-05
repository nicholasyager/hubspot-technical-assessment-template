with

customers as (
    select * from {{ ref('stg_tpch__customers') }}
),

order_items as (
    select * from {{ ref('int_order_line_items__mapped') }}
),

losses as (
    select * from {{ ref('lost_revenue') }}
),

customer_order_totals as (
    select
        customers.c_custkey as customer_id,
        customers.c_name as customer_name,
        round(sum(order_items.customer_cost), 2) as purchase_total
    from customers
    left join order_items
        on customers.c_custkey = order_items.customer_id
    where order_items.item_status != 'R'
    group by customers.c_custkey, customers.c_name
    order by customers.c_custkey
),

final as (
    select
        customer_order_totals.customer_id,
        customer_order_totals.customer_name,
        coalesce(customer_order_totals.purchase_total, 0) as purchase_total,
        coalesce(losses.revenue_lost, 0) as return_total,
        coalesce(
            coalesce(customer_order_totals.purchase_total, 0)
            - coalesce(losses.revenue_lost, 0), 0
        ) as lifetime_value,
        (coalesce(losses.revenue_lost, 0) / coalesce(purchase_total, 0))
        * 100 as return_pct,
        case
            when
                coalesce(customer_order_totals.purchase_total, 0) = 0
                then 'red' when
                (
                    coalesce(losses.revenue_lost, 0)
                    / coalesce(customer_order_totals.purchase_total, 0)
                )
                * 100
                <= 25
                then 'green'
            when
                (
                    coalesce(losses.revenue_lost, 0)
                    / coalesce(customer_order_totals.purchase_total, 0)
                )
                * 100
                <= 50
                then 'yellow'
            when
                (
                    coalesce(losses.revenue_lost, 0)
                    / coalesce(customer_order_totals.purchase_total, 0)
                )
                * 100
                <= 75
                then 'orange'
            when
                (
                    coalesce(losses.revenue_lost, 0)
                    / coalesce(customer_order_totals.purchase_total, 0)
                )
                * 100
                <= 100
                then 'red'
        end as customer_status

    from customer_order_totals
    left join losses
        on customer_order_totals.customer_id = losses.c_custkey
)

select * from final
