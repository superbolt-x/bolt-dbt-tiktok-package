{{ config (
    alias = target.database + '_tiktok_performance_by_campaign',
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
)}}

{#-
    Campaign performance, all date granularities in one table.

    Reads the day-grain incremental feeder (tiktok_performance_by_campaign_daily),
    adds date parts, rolls up day/week/month/quarter/year, and joins campaign
    metadata built inline from the raw history table. This keeps the
    tiktok_campaigns_insights / tiktok_campaigns models out of this model's DAG
    so a selective run stays slim (mirrors the Facebook performance_by_ad_daily
    + performance_by_ad pattern).
-#}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key','secondary_goal_result','secondary_goal_result_rate','cost_per_secondary_goal_result','dpa_target_audience_type'] -%}
{%- set dimensions = ['campaign_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('tiktok_performance_by_campaign_daily'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}

WITH
    insights_stg AS
    (SELECT *,
        {{ get_date_parts('date') }}
    FROM {{ ref('tiktok_performance_by_campaign_daily') }}
    {% if is_incremental() -%}
    -- Reprocess whole periods from the start of the year containing (max date - 30d)
    -- so the week/month/quarter/year roll-ups stay complete; older data is stable.
    -- Run --full-refresh periodically to refresh campaign names on historical rows.
    WHERE date >= date_trunc('year', (select dateadd(day,-30,max(date)) from {{ ref('tiktok_performance_by_campaign_daily') }}))::date
    {%- endif %}
    ),

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

    {#- campaign metadata, built inline from the raw history table
        (same logic as the tiktok_campaigns model) -#}
    {%- set campaign_fields = ['campaign_id','campaign_name','secondary_status','updated_at'] -%}
    campaigns_staging AS
    (SELECT
        {% for field in campaign_fields -%}
        {{ get_tiktok_clean_field('campaigns', field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY campaign_id) as last_updated_at
    FROM {{ source('tiktok_raw', 'campaigns') }}
    ),

    campaigns AS
    (SELECT campaign_id, campaign_name, secondary_status as campaign_status
    FROM campaigns_staging
    WHERE updated_at = last_updated_at)

SELECT *,
    {{ get_tiktok_default_campaign_types('campaign_name')}},
    md5(
        coalesce(date_granularity,'')||'|'||
        coalesce(date::varchar,'')||'|'||
        coalesce(campaign_id::varchar,'')
    ) as unique_key
FROM
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN campaigns USING(campaign_id)
