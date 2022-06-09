with leads as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_lead') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['id']::integer as id,
        _airbyte_data['postalcode']::varchar(20) as postalcode,
        _airbyte_data['phone']::varchar(40) as phone,
        _airbyte_data['dob__c']::date as dob__c,
        _airbyte_data['sfid']::varchar(18) as sfid,
        _airbyte_data['email']::varchar(80) as email,
        _airbyte_data['name']::varchar(121) as name,
        _airbyte_data['gender__c']::varchar(255) as gender__c,
        _airbyte_data['street']::varchar(255) as street,
        _airbyte_data['city']::varchar(40) as city,
        _airbyte_data['cohort__c']::double as cohort__c,
        _airbyte_data['customer_reference_number__c']::varchar(30) as customer_reference_number__c,
        _airbyte_data['application_received__c']::date as application_received__c,
        _airbyte_data['address_json__c']::varchar(1000) as address_json__c,
        _airbyte_data['country_of_residence__c']::varchar(50) as country_of_residence__c,
        _airbyte_data['account_closed__c']::boolean as account_closed__c,
        _airbyte_data['age__c']::double as age__c,
        _airbyte_data['utm_source__c']::varchar(255) as utm_source__c,
        _airbyte_data['utm_campaign__c']::varchar(255) as utm_campaign__c,
        _airbyte_data['utm_medium__c']::varchar(255) as utm_medium__c,
        _airbyte_data['initial_referring_domain__c']::varchar(255) as initial_referring_domain__c,
        _airbyte_data['createddate']::timestamp_ntz as createddate,
        _airbyte_data['mobile_app_sign_up__c']::boolean as mobile_app_sign_up__c

    from
        leads
),

ranged as (
    select
        id,
        postalcode,
        phone,
        dob__c,
        sfid,
        email,
        name,
        gender__c,
        street,
        city,
        cohort__c,
        customer_reference_number__c,
        application_received__c,
        address_json__c,
        country_of_residence__c,
        account_closed__c,
        age__c,
        utm_source__c,
        utm_campaign__c,
        utm_medium__c,
        initial_referring_domain__c,
        createddate,
        mobile_app_sign_up__c,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        id,
        postalcode,
        phone,
        dob__c,
        sfid,
        email,
        name,
        gender__c,
        street,
        city,
        cohort__c,
        customer_reference_number__c,
        application_received__c,
        address_json__c,
        country_of_residence__c,
        account_closed__c,
        age__c,
        utm_source__c,
        utm_campaign__c,
        utm_medium__c,
        initial_referring_domain__c,
        createddate,
        mobile_app_sign_up__c
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
