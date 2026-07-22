{{ config (
    alias = target.database + '_tiktok_performance_by_ad',
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
)}}

{#-
    Ad performance, all date granularities in one table.

    Reads the day-grain incremental staging model (_stg_tiktok_ads_insights)
    directly, applies currency conversion, adds date parts, rolls up
    day/week/month/quarter/year, and joins ad / adgroup / campaign metadata built
    inline from the raw history tables. Keeping the currency + rollup + metadata
    all in this one model means a selective run only materializes
    _stg_tiktok_ads_insights + this model (mirrors the Facebook day-grain +
    rollup pattern; the _stg model plays the day-grain feeder role).
-#}

{%- set currency_fields = [
    "spend",
    "add_to_wishlist_rate",
    "app_event_add_to_cart_rate",
    "checkout_rate",
    "complete_payment_rate",
    "download_start_rate",
    "initiate_checkout_rate",
    "onsite_on_web_cart_rate",
    "onsite_on_web_detail_rate",
    "onsite_shopping_rate",
    "product_details_page_browse_rate",
    "profile_visits_rate",
    "purchase_rate",
    "total_complete_payment_rate",
    "view_content_rate",
    "web_event_add_to_cart_rate"
]
-%}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key','secondary_goal_result','secondary_goal_result_rate','cost_per_secondary_goal_result','dpa_target_audience_type'] -%}
{%- set dimensions = ['ad_id'] -%}
{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_tiktok_ads_insights'))
                    |map(attribute="name")
                    |list
                    -%}
{%- set measures = stg_fields|reject("in",exclude_fields)|reject("in",dimensions)|list -%}

WITH
    {% if var('currency') != 'USD' -%}
    currency AS
    (SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate,
        LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
    FROM utilities.dates
    LEFT JOIN utilities.currency USING(date)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}

    insights AS
    (SELECT
        {%- for field in stg_fields %}
        {%- if field in currency_fields or '_value' in field %}
        COALESCE(TRY_CAST("{{ field }}" AS float), 0)/{{ exchange_rate }} as "{{ field }}"
        {%- else %}
        "{{ field }}"
        {%- endif -%}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ ref('_stg_tiktok_ads_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    {% if is_incremental() -%}
    -- Reprocess whole periods from the start of the year containing (max date - 30d)
    -- so the week/month/quarter/year roll-ups stay complete; older data is stable.
    -- Run --full-refresh periodically to refresh ad-object names on historical rows.
    WHERE date >= date_trunc('year', (select dateadd(day,-30,max(date)) from {{ ref('_stg_tiktok_ads_insights') }}))::date
    {%- endif %}
    ),

    insights_stg AS
    (SELECT *,
        {{ get_date_parts('date') }}
    FROM insights),

    {%- for date_granularity in date_granularity_list %}
    performance_{{date_granularity}} AS
    (SELECT
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        {%- for dimension in dimensions %}
        {{ dimension }},
        {%-  endfor %}
        {% for measure in measures -%}
        COALESCE(SUM("{{ measure }}"),0) as "{{ measure }}"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM insights_stg
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    {#- ad / adgroup / campaign metadata, built inline from the raw history tables
        (same logic as the tiktok_ads / tiktok_adgroups / tiktok_campaigns models) -#}
    {%- set ad_fields = ['ad_id','adgroup_id','advertiser_id','ad_name','secondary_status','updated_at'] -%}
    ads_staging AS
    (SELECT
        {% for field in ad_fields -%}
        {{ get_tiktok_clean_field('ads', field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY ad_id) as last_updated_at
    FROM {{ source('tiktok_raw', 'ads') }}
    ),

    {%- set adgroup_fields = ['adgroup_id','campaign_id','adgroup_name','secondary_status','updated_at'] -%}
    adgroups_staging AS
    (SELECT
        {% for field in adgroup_fields -%}
        {{ get_tiktok_clean_field('adgroups', field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY adgroup_id) as last_updated_at
    FROM {{ source('tiktok_raw', 'adgroups') }}
    ),

    {%- set campaign_fields = ['campaign_id','campaign_name','secondary_status','updated_at'] -%}
    campaigns_staging AS
    (SELECT
        {% for field in campaign_fields -%}
        {{ get_tiktok_clean_field('campaigns', field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY campaign_id) as last_updated_at
    FROM {{ source('tiktok_raw', 'campaigns') }}
    ),

    ads AS
    (SELECT ad_id, adgroup_id, advertiser_id, ad_name, secondary_status as ad_status
    FROM ads_staging
    WHERE updated_at = last_updated_at),

    adgroups AS
    (SELECT adgroup_id, campaign_id, adgroup_name, secondary_status as adgroup_status
    FROM adgroups_staging
    WHERE updated_at = last_updated_at),

    campaigns AS
    (SELECT campaign_id, campaign_name, secondary_status as campaign_status
    FROM campaigns_staging
    WHERE updated_at = last_updated_at)

SELECT *,
    {{ get_tiktok_default_campaign_types('campaign_name')}},
    {{ get_tiktok_scoring_objects() }},
    md5(
        coalesce(date_granularity,'')||'|'||
        coalesce(date::varchar,'')||'|'||
        coalesce(ad_id::varchar,'')
    ) as unique_key
FROM
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN ads USING(ad_id)
LEFT JOIN adgroups USING(adgroup_id)
LEFT JOIN campaigns USING(campaign_id)
