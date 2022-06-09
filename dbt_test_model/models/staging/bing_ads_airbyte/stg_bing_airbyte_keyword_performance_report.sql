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
        _airbyte_data['Keyword']::varchar as keyword,
        _airbyte_data['Impressions']::integer as impressions,
        _airbyte_data['Clicks']::integer as clicks,
        _airbyte_data['Spend']::integer as spend,
        to_date(left(_airbyte_data['TimePeriod'], 10)) as date
    from source
),

final as (
    select
        concat('bing_keyword_', keyword_id, '_', date) as key,
        to_varchar(keyword_id, 'TM9') as keyword_id,
        keyword,
        campaign_name,
        campaign_id,
        date,
        sum(impressions) as impressions,
        sum(clicks) as taps,
        sum(spend) as cost
    from
        extracted
    group by
        key, keyword_id, keyword, campaign_name, campaign_id, date
)

select * from final
