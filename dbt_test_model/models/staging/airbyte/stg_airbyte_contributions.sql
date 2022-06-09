with pensions as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_contribution') }}
),

extracted as (
    select
        convert_timezone('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['id']::integer as id,
        _airbyte_data['sfid']::varchar(18) as sfid,
        _airbyte_data['account__c']::varchar(18) as account__c,
        _airbyte_data['amount_net__c']::double as amount_net__c,
        _airbyte_data['blanket_contribution__c']::boolean as blanket_contribution__c,
        _airbyte_data['contribution_direct_from_employer__c']::boolean as contribution_direct_from_employer__c,
        _airbyte_data['contribution_type__c']::varchar(255) as contribution_type__c,
        _airbyte_data['frequency__c']::varchar(255) as frequency__c,
        _airbyte_data['is_direct_debit__c']::boolean as is_direct_debit__c,
        _airbyte_data['source_of_funds__c']::varchar(255) as source_of_funds__c,
        _airbyte_data['number_of_transactions__c']::integer as number_of_transactions__c,
        _airbyte_data['createddate']::timestamp_ntz as createddate,
        _airbyte_data['contribution_updated__c']::timestamp_ntz as contribution_updated__c,
        _airbyte_data['name']::varchar(255) as name

    from
        pensions
),

ranged as (
    select
        id,
        sfid,
        account__c,
        amount_net__c,
        blanket_contribution__c,
        contribution_direct_from_employer__c,
        contribution_type__c,
        frequency__c,
        is_direct_debit__c,
        source_of_funds__c,
        number_of_transactions__c,
        createddate,
        contribution_updated__c,
        name,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        id,
        sfid,
        account__c,
        amount_net__c,
        blanket_contribution__c,
        contribution_direct_from_employer__c,
        contribution_type__c,
        frequency__c,
        is_direct_debit__c,
        source_of_funds__c,
        number_of_transactions__c,
        createddate,
        contribution_updated__c,
        name
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
