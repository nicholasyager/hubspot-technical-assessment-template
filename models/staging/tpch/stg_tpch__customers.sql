with

source as (
    select * from {{ source('TPCH_SF1', 'customer') }}
)

select * from source
