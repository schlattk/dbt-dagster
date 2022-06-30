-- {
--   "Account__c": null,
--   "Appointment_Timeslot__c": null,
--   "CreatedById": "00524000001PONbAAO",
--   "CreatedDate": "2022-06-01T00:13:31.000Z",
--   "Email__c": "jenniemc56@gmail.com",
--   "External_ID__c": "eab68b3c-1a64-4fa7-9ac1-d07b308af07c",
--   "First_Name__c": null,
--   "Guidance_Appointment_Choice_Timestamp__c": "2022-06-01T00:13:47.000Z",
--   "Guidance_Appointment_Choice__c": "Customer Web Booking",
--   "Guidance_Outcome__c": null,
--   "Id": "a164H00000JK5tnQAD",
--   "IsDeleted": false,
--   "LastActivityDate": null,
--   "LastModifiedById": "00524000001PONbAAO",
--   "LastModifiedDate": "2022-06-01T00:13:49.000Z",
--   "Lead__c": "00Q4H00000q0pbAUAQ",
--   "Name": "SN-000039",
--   "OwnerId": "00524000001PONbAAO",
--   "Owner_Email__c": null,
--   "Qualifying_Answer__c": "Yes",
--   "Reason_for_Declining__c": null,
--   "Risk_Confirmed_Timestamp__c": null,
--   "Risk_Future_Contributions__c": null,
--   "Risk_Income_Tax__c": null,
--   "Risk_Inheritance_Tax__c": null,
--   "Risk_Invest_Elsewhere__c": null,
--   "Risk_Maintain_Lifestyle__c": null,
--   "Risk_Other_Products__c": null,
--   "Risk_With_Guarantees__c": null,
--   "Send_Guidance_Form__c": false,
--   "Send_Risks_Form__c": false,
--   "Stronger_Nudge_Status__c": "Active",
--   "Surfacing_Event__c": "Sign Up",
--   "SystemModstamp": "2022-06-01T00:13:49.000Z"
-- }


with stronger_nudge as (
    select * from {{ source('test_schema', '_airbyte_raw_stronger_nudge__c') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['Id']::varchar(255) as id,
        _airbyte_data['Account__c']::varchar(255) as account__c,
        _airbyte_data['Lead__c']::varchar(255) as lead__c,
        _airbyte_data['CreatedDate']::date as created_date,
        _airbyte_data['Guidance_Appointment_Choice_Timestamp__c']::date as guidance_appointment_date,
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
        lead__c,
        created_date,
        guidance_appointment_date,
        guidance_appointment_choice,
        stronger_nudge_status,
        surfacing_event
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

is_current as (
    select
        *,
        case when rank() over (partition by id order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
