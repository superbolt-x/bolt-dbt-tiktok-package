{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'supermetrics_raw', 'campaign_report_daily_region' -%}

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
        date,
        region,
        add_billing as add_billing_events,
        app_install as app_install_events,
        button_click as button_click_events,
        clicks as clicks,
        comments as paid_comments,
        complete_payment as complete_payment_events,
        consultation as consultation_events,
        conversions as conversions,
        cost as cost,
        download_detail as download_detail_page_events,
        follows as paid_follows,
        form as form_submission_events,
        form_detail as form_detail_page_browse_events,
        impressions as impressions,
        initiate_checkout as initiate_checkout_events,
        likes as paid_likes,
        on_web_order as web_place_order_events,
        on_web_subscribe as web_subscribe_events,
        online_consult as online_consultation_events,
        page_browse_consultation as page_browse_consultation_events,
        page_browse_view as page_browse_events,
        product_details_page_browse as product_details_page_browse_events,
        profile_visits as paid_profile_visits,
        real_time_app_install as real_time_app_installs,
        real_time_conversions as real_time_conversions,
        real_time_results as real_time_results,
        results as results,
        secondary_goal_result as secondary_goal_result,
        shares as paid_shares,
        total_achieve_level as level_achieve_events,
        total_achieve_level_value as level_achieve_value,
        total_add_billing_value as add_billing_value,
        total_add_payment_info as add_payment_info_events,
        total_add_to_wishlist as add_to_wishlist_events,
        total_add_to_wishlist_value as add_to_wishlist_value,
        total_app_event_add_to_cart as app_add_to_cart_events,
        total_app_event_add_to_cart_value as app_add_to_cart_value,
        total_button_click_consultation_value as button_click_consultation_value,
        total_button_click_value as button_click_value,
        total_checkout as checkout_events,
        total_checkout_value as checkout_value,
        total_complete_payment_rate as complete_payment_value,
        total_complete_tutorial as complete_tutorial_events,
        total_complete_tutorial_value as complete_tutorial_value,
        total_consultation_value as consultation_value,
        total_create_gamerole as create_role_events,
        total_create_gamerole_value as create_role_value,
        total_create_group as create_group_events,
        total_create_group_value as create_group_value,
        total_download_detail_value as download_detail_page_value,
        total_download_start_value as download_button_value,
        total_form_button_value as form_button_clicks_value,
        total_form_detail_value as form_detail_page_browse_value,
        total_form_value as form_submission_value,
        total_in_app_ad_click as in_app_ad_clicks,
        total_in_app_ad_click_value as in_app_ad_click_value,
        total_in_app_ad_impr as in_app_ad_impressions,
        total_in_app_ad_impr_value as in_app_ad_impressions_value,
        total_initiate_checkout_value as initiate_checkout_value,
        total_join_group as join_group_events,
        total_join_group_value as join_group_value,
        total_launch_app as launch_app_events,
        total_loan_apply as loan_apply_events,
        total_loan_credit as loan_approval_events,
        total_loan_disbursement as loan_disbursement_events,
        total_login as login_events,
        total_next_day_open as next_day_open_events,
        total_on_web_order_value as web_place_order_value,
        total_online_consult_value as online_consultation_value,
        total_page_browse_consultation_value as page_browse_consultation_value,
        total_page_event_search_value as page_search_value,
        total_product_details_page_browse_value as product_details_page_browse_value,
        total_purchase as purchase_events,
        total_purchase_value as purchase_value,
        total_registration as registration_events,
        page_event_search as search_events,
        total_spend_credits as spend_credits_events,
        total_spend_credits_value as spend_credits_value,
        total_start_trial as start_trial_events,
        total_subscribe as subscribe_events,
        total_subscribe_value as subscribe_value,
        total_unlock_achievement as unlock_achievement_events,
        total_unlock_achievement_value as unlock_achievement_value,
        total_user_registration_value as user_registration_value,
        total_view_content as view_content_events,
        total_view_content_value as view_content_value,
        total_web_event_add_to_cart_value as web_add_to_cart_value,
        user_registration as user_registration_events,
        video_play_actions as video_play_actions,
        video_views_p100 as video_views_p100,
        video_views_p25 as video_views_p25,
        video_views_p50 as video_views_p50,
        video_views_p75 as video_views_p75,
        video_watched_2s as video_watched_2s,
        video_watched_6s as video_watched_6s,
        web_event_add_to_cart as web_add_to_cart_events
     FROM insights)
     
SELECT *,
    MAX(date) over () as last_updated,
    campaign_id||'_'||region||'_'||date as unique_key
FROM cleaned_insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-30 from {{ this }})

{% endif %}
