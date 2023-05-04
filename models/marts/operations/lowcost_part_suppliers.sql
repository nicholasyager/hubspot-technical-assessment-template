with

parts as (
    select
        part_id,
        part_name,
        size,
        part_manufacturer,
        retail_price
    from {{ ref('stg_tpch__parts') }}
),

dim_suppliers as (
    select
        supplier_id,
        supplier_name,
        supplier_address,
        phone,
        account_balance,
        nation_name,
        region_name,
        supplier_comment
    from {{ ref('suppliers') }}
),

part_suppliers as (
    select
        part_id,
        supplier_id,
        supply_cost
    from {{ ref('stg_tpch__part_suppliers') }}
),

final as (
    select
        p.part_id,
        p.part_name,
        p.size,
        p.part_manufacturer,
        p.retail_price,
        ps.supply_cost,
        s.supplier_id,
        s.account_balance,
        s.supplier_name,
        s.nation_name,
        s.region_name,
        s.supplier_address,
        s.phone,
        s.supplier_comment
    from parts as p
    left join part_suppliers as ps
        on p.part_id = ps.part_id
    left join dim_suppliers as s
        on ps.supplier_id = s.supplier_id
    qualify
        row_number() over (
            partition by
                p.part_id,
                s.nation_name
            order by ps.supply_cost desc
        ) = 1
)

select * from final
