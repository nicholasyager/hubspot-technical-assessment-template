with

source as (
    select
        p_partkey as part_id,
        p_name as part_name,
        p_mfgr as part_manufacturer,
        p_brand as brand,
        p_type as part_type,
        p_size as size,
        p_container as container,
        p_retailprice as retail_price,
        p_comment as part_comment
    from {{ source('TPCH_SF1', 'part') }}
)

select * from source
