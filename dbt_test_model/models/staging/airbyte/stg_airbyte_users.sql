with users as (
    select * from {{ source('public', '_airbyte_raw_postgres_airbyte_user') }}
),

extracted as (
    select
        CONVERT_TIMEZONE('UTC', _airbyte_emitted_at) as _airbyte_emitted_at,
        _airbyte_data['id']::integer as id,
        _airbyte_data['email']::varchar(80) as email,
        _airbyte_data['role']::varchar(40) as role,
        CONVERT_TIMEZONE('UTC', _airbyte_data['createdAt']::timestamp) as created_at,
        CONVERT_TIMEZONE('UTC', _airbyte_data['updatedAt']::timestamp) as updated_at

    from
        users
),

ranged as (
    select
        id,
        email,
        role,
        created_at,
        updated_at,
        min(_airbyte_emitted_at) as _airbyte_first_emitted,
        max(_airbyte_emitted_at) as _airbyte_last_emitted,
        count(distinct _airbyte_emitted_at) as _airbyte_emitted_count

    from
        extracted

    group by
        id,
        email,
        role,
        created_at,
        updated_at
),

is_current as (
    select
        *,
        case when rank() over (partition by email order by _airbyte_last_emitted desc) = 1 then true else false end as _airbyte_is_current

    from ranged
)

select * from is_current
