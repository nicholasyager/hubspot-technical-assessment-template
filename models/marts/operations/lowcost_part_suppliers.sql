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
        parts.part_id,
        parts.part_name,
        parts.size,
        parts.part_manufacturer,
        parts.retail_price,
        part_suppliers.supply_cost,
        dim_suppliers.supplier_id,
        dim_suppliers.account_balance,
        dim_suppliers.supplier_name,
        dim_suppliers.nation_name,
        dim_suppliers.region_name,
        dim_suppliers.supplier_address,
        dim_suppliers.phone,
        dim_suppliers.supplier_comment
    from parts
    left join part_suppliers
        on parts.part_id = part_suppliers.part_id
    left join dim_suppliers
        on part_suppliers.supplier_id = dim_suppliers.supplier_id
    qualify
        row_number() over (
            partition by
                parts.part_id,
                dim_suppliers.nation_name
            order by part_suppliers.supply_cost desc
        ) = 1
)

select * from final
