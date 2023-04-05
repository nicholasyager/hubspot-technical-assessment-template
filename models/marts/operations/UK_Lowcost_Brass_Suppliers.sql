select
    e.p_name as part_name,
    e.p_retailprice as retailprice,
    e.s_name as supplier_name,
    e.p_mfgr as part_manufacturer,
    e.s_address as suppaddr,
    e.s_phone as supp_phone,
    ps.ps_availqty as num_available

from {{ ref('EUR_LOWCOST_BRASS_SUPPLIERS') }} as e
left join {{ source('TPCH_SF1', 'supplier') }} as s on e.s_name = s.s_name
left join sources.partsupp as ps
    on
        e.p_partkey = ps.ps_partkey
        and s.s_suppkey = ps.ps_suppkey

where n_name = 'UNITED KINGDOM'
