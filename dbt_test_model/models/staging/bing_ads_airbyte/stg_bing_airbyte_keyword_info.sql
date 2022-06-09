with

keyword_report as (
    select * from {{ ref('stg_bing_airbyte_network_keyword_performance_report') }}
),

ad_group_info as (
    select * from {{ ref('stg_bing_airbyte_ad_groups') }}
),

final as (
    select
        keyword_report.*,
        ad_group_info.bid_strategy_type
    from
        keyword_report
    left join
        ad_group_info
    using(ad_group_id)
)

select * from final
