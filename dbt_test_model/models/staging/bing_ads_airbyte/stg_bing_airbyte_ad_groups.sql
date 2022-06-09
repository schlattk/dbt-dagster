with

source as (
    select * from {{ source('ads', '_airbyte_raw_bing_ad_groups') }}
),

extracted as (
    select
        _airbyte_data['Id']::integer as ad_group_id,
        _airbyte_data['Name']::varchar as ad_group_name,
        _airbyte_data['BiddingScheme']['InheritedBidStrategyType']::varchar as bid_strategy_type
    from source
),

final as (
    select
        ad_group_id, bid_strategy_type
    from
        extracted
    group by
        ad_group_id, bid_strategy_type
)

select * from final
