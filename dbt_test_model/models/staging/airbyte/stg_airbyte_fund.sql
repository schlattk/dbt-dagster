{{ config(materialized='table') }}


with funds as (
    select * from {{ source('test_schema', '_airbyte_raw_fund__c') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['Id']::varchar(255) as id,
        _airbyte_data['Name']::varchar(255) as name,
        _airbyte_data['Total_AUM__c']::double as total_aum,
        left(_airbyte_data['CreatedDate'], 19)::timestamp as created_date
    from
        funds
),

ranged as (
    select
        id,
        name,
        total_aum,
        created_date,
        max(_airbyte_emitted_at) as _airbyte_last_emitted
    from
        extracted

    group by
        id,
        name,
        total_aum,
        created_date
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
