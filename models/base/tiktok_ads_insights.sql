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

{%- set exclude_fields = [
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

SELECT *,
    {{ get_date_parts('date') }}
FROM insights 
