select
    e.part_name,
    e.retail_price as retailprice,
    e.supplier_name,
    e.part_manufacturer,
    e.supplier_address as suppaddr,
    e.phone as supp_phone,
    ps.ps_availqty as num_available

from {{ ref('EUR_LOWCOST_BRASS_SUPPLIERS') }} as e
left join {{ source('TPCH_SF1', 'supplier') }} as s on e.supplier_name = s.s_name
left join sources.partsupp as ps
    on
        e.part_id = ps.ps_partkey
        and s.s_suppkey = ps.ps_suppkey

where nation_name = 'UNITED KINGDOM'
