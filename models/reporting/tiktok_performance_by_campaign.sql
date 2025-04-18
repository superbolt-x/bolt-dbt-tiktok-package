{{ config (
    alias = target.database + '_tiktok_performance_by_campaign'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key','secondary_goal_result','secondary_goal_result_rate','cost_per_secondary_goal_result','dpa_target_audience_type'] -%}
{%- set dimensions = ['campaign_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('tiktok_campaigns_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  

WITH 
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
    FROM {{ ref('tiktok_campaigns_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    campaigns AS 
    (SELECT campaign_id, campaign_name, secondary_status as campaign_status
    FROM {{ ref('tiktok_campaigns') }}
    )

SELECT *,
    {{ get_tiktok_default_campaign_types('campaign_name')}}
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN campaigns USING(campaign_id)
