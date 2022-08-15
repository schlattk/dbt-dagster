{{ config(materialized='table') }}

with

source as (
    select * from {{ ref('stg_airbyte_fund') }}
),

deduplicated as (
    select
        *
    from
        source
    where
        _airbyte_is_current = true
)

select * from deduplicated
