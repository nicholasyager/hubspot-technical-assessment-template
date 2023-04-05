with

source as (
    select * from {{ source('TPCH_SF1', 'partsupp') }}
)

select * from source
