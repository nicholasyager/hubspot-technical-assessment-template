with

source as (
    select
        n_nationkey as nation_id,
        n_name as nation_name,
        n_regionkey as region_id,
        n_comment as nation_comment
    from {{ source('TPCH_SF1', 'nation') }}
)

select * from source
