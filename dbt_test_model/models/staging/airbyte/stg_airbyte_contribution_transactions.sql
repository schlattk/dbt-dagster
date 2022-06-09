with pensions as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_contribution_transaction') }}
),

extracted as (
    select
        convert_timezone('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['account__c']::varchar(18) as account__c,
        _airbyte_data['actual_value__c']::double as actual_value__c,
        _airbyte_data['completed_date__c']::timestamp_ntz as completed_date__c,
        _airbyte_data['id']::integer as id,
        _airbyte_data['ras_actual_value__c']::double as ras_actual_value__c,
        _airbyte_data['received_date__c']::timestamp_ntz as received_date__c,
        _airbyte_data['sfid']::varchar(18) as sfid,
        _airbyte_data['sum_of_actual_value_plus_ras_value__c']::double as sum_of_actual_value_plus_ras_value__c,
        _airbyte_data['contribution__c']::varchar(18) as contribution__c

    from
        pensions
),

ranged as (
    select
        account__c,
        actual_value__c,
        completed_date__c,
        id,
        ras_actual_value__c,
        received_date__c,
        sfid,
        sum_of_actual_value_plus_ras_value__c,
        contribution__c,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        account__c,
        actual_value__c,
        completed_date__c,
        id,
        ras_actual_value__c,
        received_date__c,
        sfid,
        sum_of_actual_value_plus_ras_value__c,
        contribution__c
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
