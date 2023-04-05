with

source as (
    select * from {{ source('TPCH_SF1', 'region') }}
)

select * from source
