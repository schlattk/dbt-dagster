with

source as (
    select * from {{ source('ads', '_airbyte_raw_bing_keyword_performance_report_daily') }}
),

extracted as (
    select
        to_timestamp(left(_airbyte_emitted_at, 10)) as _airbyte_emitted_at,
        _airbyte_data['CampaignId']::integer as campaign_id,
        _airbyte_data['CampaignName']::varchar as campaign_name,
        _airbyte_data['KeywordId']::integer as keyword_id,
        _airbyte_data['Keyword']::varchar as keyword_name,
        _airbyte_data['AdId']::integer as ad_id,
        _airbyte_data['AdGroupId']::integer as ad_group_id,
        _airbyte_data['AdGroupName']::varchar as ad_group_name,
        _airbyte_data['Network']::varchar as network,
        _airbyte_data['Impressions']::integer as impressions,
        _airbyte_data['Clicks']::integer as clicks,
        _airbyte_data['Spend']::float as spend,
        to_date(left(_airbyte_data['TimePeriod'], 10)) as date
    from source
),

-- this is necessary to maintain key uniqueness. before this date, the same key combination is delivered with different
-- bid_match_type, but we do not have a way of knowing the bid match type on the touch side, so this needs to be unique.

-- update: the duplication happens after this date as well, so we need to remove bid_match_type
filtered as (
    select * from extracted where date > '2021-05-01'
),

final as (
    select
        to_varchar(ad_group_id, 'TM9') as ad_group_id,
        concat('bing_', keyword_id, '_', ad_id, '_', network, '_', date) as key,
        to_varchar(keyword_id, 'TM9') as keyword_id,
        keyword_name,
        to_varchar(campaign_id, 'TM9') as campaign_id,
        campaign_name,
        to_varchar(ad_id, 'TM9') as ad_id,
        ad_group_name,
        network,
        -- bid_match_type as match_type, -- remove as we cannot match to this level in the touch data & it causes key duplication
        date,
        sum(impressions) as impressions,
        sum(clicks) as clicks,
        sum(spend) as cost
    from
        filtered
    group by
        ad_group_id, key, keyword_id, keyword_name, campaign_id, campaign_name, ad_id, ad_group_name, network, date
)

select * from final
