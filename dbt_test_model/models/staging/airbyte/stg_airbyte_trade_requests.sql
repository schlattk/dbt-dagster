with trade_requests as (
    select * from {{ source('default_schema', '_airbyte_raw__salesforce_financetrade_request__c') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['Id']::varchar(255) as id,
        _airbyte_data['AccountId__c']::varchar(255) as accountid__c,
        _airbyte_data['Contribution_Transaction__c']::varchar(255) as contribution_transaction__c,
        _airbyte_data['Date_Traded__c']::date as date_traded__c,
        _airbyte_data['Fund__c']::varchar(255) as fund__c,
        _airbyte_data['Fund_Switch__c']::varchar(255) as fund_switch__c,
        _airbyte_data['Is_RAS__c']::boolean as is_ras__c,
        _airbyte_data['PensionBee_Pension__c']::varchar(255) as pensionbee_pension__c,
        _airbyte_data['Status__c']::varchar(255) as status__c,
        left(_airbyte_data['SystemModstamp'], 19)::timestamp as systemmodstamp,
        _airbyte_data['Total_Value_Due_Disinvestment__c']::double as total_value_due_disinvestment__c,
        _airbyte_data['Total_Value_Requested_Investment__c']::double as total_value_requested_investment__c,
        _airbyte_data['Trade_Request_Type__c']::varchar(255) as trade_request_type__c,
        _airbyte_data['Trade_Type__c']::varchar(255) as trade_type__c,
        _airbyte_data['Withdrawal__c']::varchar(255) as withdrawal__c,
        _airbyte_data['Pension__c']::varchar(255) as pension__c

    from
        trade_requests
),

ranged as (
    select
        accountid__c,
        contribution_transaction__c,
        date_traded__c,
        fund__c,
        fund_switch__c,
        id,
        is_ras__c,
        pensionbee_pension__c,
        status__c,
        systemmodstamp,
        total_value_due_disinvestment__c,
        total_value_requested_investment__c,
        trade_request_type__c,
        trade_type__c,
        withdrawal__c,
        pension__c,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        accountid__c,
        contribution_transaction__c,
        date_traded__c,
        fund__c,
        fund_switch__c,
        id,
        is_ras__c,
        pensionbee_pension__c,
        status__c,
        systemmodstamp,
        total_value_due_disinvestment__c,
        total_value_requested_investment__c,
        trade_request_type__c,
        trade_type__c,
        withdrawal__c,
        pension__c
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
