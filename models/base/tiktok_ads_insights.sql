{%- set currency_fields = [
    "cost",
    "level_achieve_value",
    "add_billing_value",
    "add_to_wishlist_value",
    "app_add_to_cart_value",
    "button_click_consultation_value",
    "button_click_value",
    "checkout_value",
    "total_complete_payment_value",
    "complete_tutorial_value",
    "consultation_value",
    "create_role_value",
    "create_group_value",
    "download_detail_page_value",
    "download_button_value",
    "form_button_clicks_value",
    "form_detail_page_browse_value",
    "form_submission_value",
    "in_app_ad_click_value",
    "in_app_ad_impressions_value",
    "initiate_checkout_value",
    "join_group_value",
    "web_place_order_value",
    "online_consultation_value",
    "page_browse_consultation_value",
    "page_search_value",
    "product_details_page_browse_value",
    "purchase_value",
    "spend_credits_value",
    "subscribe_value",
    "unlock_achievement_value",
    "user_registration_value",
    "view_content_value",
    "web_add_to_cart_value"
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
