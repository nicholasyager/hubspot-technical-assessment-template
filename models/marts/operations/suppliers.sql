with

suppliers as (
    select * from {{ ref('stg_tpch__suppliers') }}
),

nations as (
    select * from {{ ref('stg_tpch__nations') }}
),

regions as (
    select * from {{ ref('stg_tpch__regions') }}
),

final as (
    select
        suppliers.*,
        nations.nation_name,
        nations.region_id,
        regions.region_name
    from suppliers
    left join nations on suppliers.nation_id = nations.nation_id
    left join regions on nations.region_id = regions.region_id
)

select * from final
