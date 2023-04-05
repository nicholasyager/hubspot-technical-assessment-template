with

source as (
    select * from {{ source('TPCH_SF1', 'supplier') }}
)

select * from source
