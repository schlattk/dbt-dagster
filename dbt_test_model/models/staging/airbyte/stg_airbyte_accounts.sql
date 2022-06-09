with accounts as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_account') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['id']::integer as id,
        _airbyte_data['personmailingstreet']::varchar(255) as personmailingstreet,
        _airbyte_data['employment_status__c']::varchar(255) as employment_status__c,
        _airbyte_data['date_of_birth__c']::date as date_of_birth__c,
        _airbyte_data['account_status__c']::varchar(255) as account_status__c,
        _airbyte_data['personemail']::varchar(80) as personemail,
        _airbyte_data['gender__c']::varchar(255) as gender__c,
        _airbyte_data['nationality__c']::varchar(50) as nationality__c,
        _airbyte_data['sfid']::varchar(18) as sfid,
        _airbyte_data['personmailingcity']::varchar(40) as personmailingcity,
        _airbyte_data['phone']::varchar(40) as phone,
        _airbyte_data['personmailingpostalcode']::varchar(20) as personmailingpostalcode,
        _airbyte_data['customer_reference_number__c']::varchar(50) as customer_reference_number__c,
        _airbyte_data['new_pension_customer__c']::boolean as new_pension_customer__c,
        _airbyte_data['cohort__c']::double as cohort__c,
        _airbyte_data['account_closed__c']::boolean as account_closed__c,
        _airbyte_data['partnership__c']::varchar(255) as partnership__c,
        _airbyte_data['age__c']::double as age__c,
        _airbyte_data['number_of_raf_invites__c']::double as number_of_raf_invites__c,
        _airbyte_data['logged_into_mobile_app__c']::boolean as logged_into_mobile_app__c,
        _airbyte_data['saasquatch_referral_token__c']::varchar(255) as saasquatch_referral_token__c,
        _airbyte_data['utm_source__c']::varchar(255) as utm_source__c,
        _airbyte_data['utm_campaign__c']::varchar(255) as utm_campaign__c,
        _airbyte_data['utm_medium__c']::varchar(255) as utm_medium__c,
        _airbyte_data['initial_referring_domain__c']::varchar(255) as initial_referring_domain__c,
        _airbyte_data['createddate']::timestamp_ntz as createddate,
        _airbyte_data['mobile_app_sign_up__c']::boolean as mobile_app_sign_up__c,
        _airbyte_data['saasquatch_error__c']::varchar(255) as saasquatch_error__c,
        _airbyte_data['offered_contribution_first__c']::boolean as offered_contribution_first__c,
        _airbyte_data['first_transfer_went_live__c']::timestamp_ntz as first_transfer_went_live__c,
        _airbyte_data['region__c']::varchar(255) as region__c,
        _airbyte_data['selected_contribution_first__c']::boolean as selected_contribution_first__c,
        _airbyte_data['age_at_account_creation__c']::double as age_at_account_creation__c


    from
        accounts
),

ranged as (
    select
        id,
        personmailingstreet,
        employment_status__c,
        date_of_birth__c,
        account_status__c,
        personemail,
        gender__c,
        nationality__c,
        sfid,
        personmailingcity,
        phone,
        personmailingpostalcode,
        customer_reference_number__c,
        new_pension_customer__c,
        cohort__c,
        account_closed__c,
        partnership__c,
        age__c,
        number_of_raf_invites__c,
        logged_into_mobile_app__c,
        saasquatch_referral_token__c,
        utm_source__c,
        utm_campaign__c,
        utm_medium__c,
        initial_referring_domain__c,
        createddate,
        mobile_app_sign_up__c,
        saasquatch_error__c,
        offered_contribution_first__c,
        first_transfer_went_live__c,
        region__c,
        selected_contribution_first__c,
        age_at_account_creation__c,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        id,
        personmailingstreet,
        employment_status__c,
        date_of_birth__c,
        account_status__c,
        personemail,
        gender__c,
        nationality__c,
        sfid,
        personmailingcity,
        phone,
        personmailingpostalcode,
        customer_reference_number__c,
        new_pension_customer__c,
        cohort__c,
        account_closed__c,
        partnership__c,
        age__c,
        number_of_raf_invites__c,
        logged_into_mobile_app__c,
        saasquatch_referral_token__c,
        utm_source__c,
        utm_campaign__c,
        utm_medium__c,
        initial_referring_domain__c,
        createddate,
        mobile_app_sign_up__c,
        saasquatch_error__c,
        offered_contribution_first__c,
        first_transfer_went_live__c,
        region__c,
        selected_contribution_first__c,
        age_at_account_creation__c
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
