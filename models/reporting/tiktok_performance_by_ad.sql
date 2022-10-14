{{ config (
    alias = target.database + '_tiktok_performance_by_ad'
)}}

SELECT *,
    {{ get_date_parts('date') }},
    {{ get_tiktok_default_campaign_types('campaign_name')}},
    {{ get_tiktok_scoring_objects() }}

FROM {{ ref('tiktok_ads_insights') }}