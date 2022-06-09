with pensions as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_pensionbee_pension') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['id']::integer as id,
        _airbyte_data['sfid']::varchar(18) as sfid,
        _airbyte_data['name']::varchar(80) as name,
        _airbyte_data['account__c']::varchar(18) as account__c,
        _airbyte_data['lead__c']::varchar(18) as lead__c,
        _airbyte_data['date_started__c']::date as date_started__c,
        _airbyte_data['current_value__c']::double as current_value__c,
        _airbyte_data['plan_type__c']::varchar(255) as plan_type__c,
        _airbyte_data['fund_name__c']::varchar(1300) as fund_name__c

    from
        pensions
),

ranged as (
    select
        id,
        sfid,
        name,
        account__c,
        lead__c,
        date_started__c,
        current_value__c,
        plan_type__c,
        fund_name__c,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        id,
        sfid,
        name,
        account__c,
        lead__c,
        date_started__c,
        current_value__c,
        plan_type__c,
        fund_name__c
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
