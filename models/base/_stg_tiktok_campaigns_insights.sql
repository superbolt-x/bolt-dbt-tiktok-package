{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'tiktok_raw', 'campaign_report_daily' -%}

{%- set exclude_fields = [
   "advertiser_address",
   "advertiser_balance",
   "advertiser_company",
   "advertiser_country",
   "advertiser_currency",
   "advertiser_email",
   "advertiser_id",
   "advertiser_license_number",
   "advertiser_name",
   "advertiser_phone_number",
   "advertiser_status",
   "advertiser_timezone",
   "campaign_budget",
   "campaign_creation_date",
   "campaign_name"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH insights AS 
    (SELECT 
        {%- for field in fields if ("cost_per" not in field) %}
        {{ get_tiktok_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    ),
    
cleaned_insights AS 
    (SELECT
        campaign_id,
        add_to_wishlist as add_to_wishlist_events,
        add_to_wishlist_rate as add_to_wishlist_value,
        app_event_add_to_cart as app_add_to_cart_events,
        app_event_add_to_cart_rate as app_add_to_cart_value,
        checkout as checkout_events,
        checkout_rate as checkout_value,
        clicks,
        comments as paid_comments,
        complete_payment,
        complete_payment_rate,
        conversion as conversions,
        cta_app_install+vta_app_install as real_time_app_installs,
        cta_conversion,
        cta_purchase,
        cta_registration,
        download_start,
        download_start_rate,
        follows as paid_follows,
        impressions,
        initiate_checkout as initiate_checkout_events,
        initiate_checkout_rate as initiate_checkout_value,
        likes as paid_likes,
        on_web_add_to_wishlist,
        on_web_subscribe as subscribe_events,
        onsite_initiate_checkout_count,
        onsite_on_web_cart,
        onsite_on_web_cart_rate,
        onsite_on_web_detail,
        onsite_on_web_detail_rate,
        onsite_shopping,
        onsite_shopping_rate,
        page_event_search as search_events,
        product_details_page_browse as product_details_page_browse_events,
        product_details_page_browse_rate as product_details_page_browse_value,
        profile_visits,
        profile_visits_rate,
        purchase as purchase_events,
        purchase_rate as purchase_value,
        real_time_conversion as real_time_conversions,
        real_time_result as real_time_results,
        registration as user_registration_events,
        result as results,
        sales_lead,
        secondary_goal_result,
        shares as paid_shares,
        skan_conversion,
        skan_sales_lead,
        skan_total_sales_lead,
        skan_total_sales_lead_value,
        spend as cost,
        stat_time_day as date,
        total_add_to_wishlist,
        total_add_to_wishlist_value,
        total_app_event_add_to_cart,
        total_app_event_add_to_cart_value,
        total_checkout,
        total_checkout_value,
        total_complete_payment_rate,
        total_download_start_value as download_button_value,
        total_initiate_checkout_value,
        total_landing_page_view,
        total_on_web_add_to_wishlist_value,
        total_on_web_subscribe_value as subscribe_value,
        total_onsite_on_web_cart_value,
        total_onsite_on_web_detail_value,
        total_onsite_shopping_value,
        total_page_event_search_value as page_search_value,
        total_pageview,
        total_product_details_page_browse_value,
        total_purchase,
        total_purchase_value,
        total_registration,
        total_sales_lead,
        total_sales_lead_value,
        total_user_registration_value,
        total_view_content,
        total_view_content_value,
        total_web_event_add_to_cart_value,
        user_registration as user_registration_value,
        video_views_p_100 as video_views_p100,
        video_views_p_25 as video_views_p25,
        video_views_p_50 as video_views_p50,
        video_views_p_75 as video_views_p75,
        video_watched_2_s as video_watched_2s,
        video_watched_6_s as video_watched_6s,
        view_content as view_content_events,
        view_content_rate as view_content_value,
        vta_conversion,
        vta_purchase,
        vta_registration,
        web_event_add_to_cart as web_add_to_cart_events,
        web_event_add_to_cart_rate as web_add_to_cart_value
     FROM insights)
     
SELECT *,
    MAX(date) over () as last_updated,
    campaign_id||'_'||date as unique_key
FROM cleaned_insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-30 from {{ this }})

{% endif %}
