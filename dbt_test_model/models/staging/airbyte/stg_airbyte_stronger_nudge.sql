{{ config(materialized='table') }}


with stronger_nudge as (
    select * from {{ source('test_schema', '_airbyte_raw_stronger_nudge__c') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['Id']::varchar(255) as id,
        _airbyte_data['Account__c']::varchar(255) as account__c,
        _airbyte_data['Lead__c']::varchar(255) as lead__c,
        left(_airbyte_data['CreatedDate'], 19)::timestamp as created_date,
        left(_airbyte_data['Guidance_Appointment_Choice_Timestamp__c'], 19)::timestamp as guidance_appointment_date,
        _airbyte_data['Guidance_Appointment_Choice__c']::varchar(255) as guidance_appointment_choice,
        _airbyte_data['Stronger_Nudge_Status__c']::varchar(50) as stronger_nudge_status,
        _airbyte_data['Surfacing_Event__c']::varchar(50) as surfacing_event
    from
        stronger_nudge
),

ranged as (
    select
        id,
        account__c,
        lead__,
        created_date,
        guidance_appointment_date,
        guidance_appointment_choice,
        stronger_nudge_status,
        surfacing_event,
        max(_airbyte_emitted_at) as _airbyte_last_emitted
    from
        extracted

    group by
        id,
        account__c,
        lead__c,
        created_date,
        guidance_appointment_date,
        guidance_appointment_choice,
        stronger_nudge_status,
        surfacing_event
),

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
