{{ config (
    alias = target.database + '_tiktok_performance_by_ad'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['ad_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('tiktok_ads_insights'))
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
    FROM {{ ref('tiktok_ads_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    ads AS 
    (SELECT ad_id::VARCHAR as ad_id, adgroup_id::VARCHAR as adgroup_id, ad_name, ad_status
    FROM {{ ref('tiktok_ads') }}
    ),

    adgroups AS 
    (SELECT adgroup_id::VARCHAR as adgroup_id, campaign_id::VARCHAR as campaign_id, adgroup_name, adgroup_status
    FROM {{ ref('tiktok_adgroups') }}
    ),

    campaigns AS 
    (SELECT campaign_id::VARCHAR as campaign_id, campaign_name, campaign_status
    FROM {{ ref('tiktok_campaigns') }}
    )

SELECT *,
    {{ get_tiktok_default_campaign_types('campaign_name')}},
    {{ get_tiktok_scoring_objects() }}
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
