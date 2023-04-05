with

source as (
    select * from {{ source('TPCH_SF1', 'part') }}
)

select * from source
