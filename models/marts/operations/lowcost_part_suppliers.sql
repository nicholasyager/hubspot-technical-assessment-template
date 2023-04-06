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

suppliers as (
    select
        supplier_id,
        supplier_name,
        supplier_address,
        phone,
        account_balance,
        nation_id,
        supplier_comment
    from {{ ref('stg_tpch__suppliers') }}
),

part_suppliers as (
    select
        part_id,
        supplier_id
    from {{ ref('stg_tpch__part_suppliers') }}
),

nations as (
    select
        nation_id,
        nation_name,
        region_id
    from {{ ref('stg_tpch__nations') }}
),

regions as (
    select
        region_id,
        region_name
    from {{ ref('stg_tpch__regions') }}
),

final as (
    select
        parts.part_id,
        parts.part_name,
        parts.size,
        parts.part_manufacturer,
        parts.retail_price,
        suppliers.account_balance,
        suppliers.supplier_name,
        nations.nation_name,
        regions.region_name,
        suppliers.supplier_address,
        suppliers.phone,
        suppliers.supplier_comment
    from parts
    left join part_suppliers on parts.part_id = part_suppliers.part_id
    left join suppliers on part_suppliers.supplier_id = suppliers.supplier_id
    left join nations on suppliers.nation_id = nations.nation_id
    left join regions on nations.region_id = regions.region_id
)

select * from final
