with

source as (
    select * from {{ source('TPCH_SF1', 'lineitem') }}
)

select * from source
