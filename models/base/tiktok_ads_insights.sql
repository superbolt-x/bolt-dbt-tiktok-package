{%- set currency_fields = [
    "spend",
    "total_purchase_value"
]
-%}

{%- set exclude_fields = [
    "_fivetran_synced",
    "total_sales_lead",
    "secondary_goal_result",
    "video_views_p_25",
    "video_views_p_75",
    "profile_visits",
    "skan_total_sales_lead",
    "skan_total_sales_lead_value",
    "likes",
    "comments",
    "video_views_p_50",
    "video_watched_2_s",
    "skan_sales_lead",
    "follows",
    "video_watched_6_s",
    "shares",
    "total_sales_lead_value",
    "skan_conversion",
    "average_video_play_per_user"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_tiktok_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

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
        {%- for field in stg_fields -%}
        {%- if field in currency_fields or '_value' in field %}
        "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
        {%- else %}
        "{{ field }}"
        {%- endif -%}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ ref('_stg_tiktok_ads_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM insights 