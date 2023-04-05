with

source as (
    select * from {{ source('TPCH_SF1', 'nation') }}
)

select * from source
