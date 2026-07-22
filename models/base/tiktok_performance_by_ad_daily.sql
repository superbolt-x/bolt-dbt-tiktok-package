{{ config(
        materialized='incremental',
        unique_key='unique_key',
        on_schema_change='append_new_columns'
) }}

{#-
    Day-grain feeder for tiktok_performance_by_ad (mirrors the Facebook
    performance_by_ad_daily + performance_by_ad split).

    Reads the cleaned day-grain staging model and applies currency conversion,
    so the reporting model can read a single physical, incremental day-grain
    table and roll the coarser grains up from it. No aggregation happens here —
    ad_report_daily is already one row per ad x date. Incremental, 30-day
    lookback.
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

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_tiktok_ads_insights'))
                    |map(attribute="name")
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
    )

SELECT *
FROM insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-30 from {{ this }})

{% endif %}
