with

source as (
    select * from {{ source('TPCH_SF1', 'orders') }}
)

select * from source
