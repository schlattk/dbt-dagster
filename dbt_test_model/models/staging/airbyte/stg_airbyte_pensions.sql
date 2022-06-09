with pensions as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_pension') }}
),

extracted as (
    select
        convert_timezone('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['id']::integer as id,
        _airbyte_data['sfid']::varchar(18) as sfid,
        _airbyte_data['account__c']::varchar(18) as account__c,
        _airbyte_data['actual_transferred_value__c']::double as actual_transferred_value__c,
        _airbyte_data['transfer_confirmation_received__c']::timestamp_ntz as transfer_confirmation_received__c,
        _airbyte_data['transfer_went_live__c']::timestamp_ntz as transfer_went_live__c,
        _airbyte_data['systemmodstamp']::timestamp_ntz as systemmodstamp,
        _airbyte_data['createddate']::timestamp_ntz as createddate,
        _airbyte_data['generic_provider__c']::varchar(256) as generic_provider__c

    from
        pensions
),

ranged as (
    select
        id,
        sfid,
        account__c,
        actual_transferred_value__c,
        transfer_confirmation_received__c,
        transfer_went_live__c,
        systemmodstamp,
        createddate,
        generic_provider__c,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        id,
        sfid,
        account__c,
        actual_transferred_value__c,
        transfer_confirmation_received__c,
        transfer_went_live__c,
        systemmodstamp,
        createddate,
        generic_provider__c
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
