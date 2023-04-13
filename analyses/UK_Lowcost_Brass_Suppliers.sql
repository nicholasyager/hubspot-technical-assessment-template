with

lowcost_suppliers as (
    select * from {{ ref('lowcost_part_suppliers') }}
),

all_suppliers as (
    select * from {{ ref('suppliers') }}
),

parts as (
    select * from {{ ref('stg_tpch__parts') }}
),


eur_lowcost_brass_suppliers as (

    select
        lowcost_suppliers.part_name,
        lowcost_suppliers.size,
        lowcost_suppliers.retail_price,
        lowcost_suppliers.account_balance,
        lowcost_suppliers.supplier_name,
        lowcost_suppliers.nation_name,
        lowcost_suppliers.part_id,
        lowcost_suppliers.part_manufacturer,
        lowcost_suppliers.supplier_address,
        lowcost_suppliers.phone,
        lowcost_suppliers.supplier_comment
    from lowcost_suppliers
    left join parts
        on lowcost_suppliers.part_id = parts.part_id
    left join all_suppliers
        on lowcost_suppliers.supplier_id = all_suppliers.supplier_id
    where
        lowcost_suppliers.size = 15
        and parts.part_type in (
            'LARGE BRUSHED BRASS',
            'SMALL PLATED BRASS',
            'PROMO ANODIZED BRASS'
        )
        and lowcost_suppliers.region_name = 'EUROPE'

)

select
    e.part_name,
    e.retail_price as retailprice,
    e.supplier_name,
    e.part_manufacturer,
    e.supplier_address as suppaddr,
    e.phone as supp_phone,
    ps.ps_availqty as num_available

from eur_lowcost_brass_suppliers as e
left join {{ source('TPCH_SF1', 'supplier') }} as s on e.supplier_name = s.s_name
left join sources.partsupp as ps
    on
        e.part_id = ps.ps_partkey
        and s.s_suppkey = ps.ps_suppkey

where nation_name = 'UNITED KINGDOM'
