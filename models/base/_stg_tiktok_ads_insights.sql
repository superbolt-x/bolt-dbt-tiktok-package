{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'tiktok_raw', 'ad_report_daily' -%}

{%- set exclude_fields = [
   "ad_group_id",
   "ad_group_name",
   "ad_group_opt_status",
   "ad_group_status",
   "ad_name",
   "ad_opt_status",
   "ad_status",
   "ad_text",
   "adgroup_placement",
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
   "app_download_url",
   "app_id",
   "app_name",
   "app_package",
   "app_type",
   "audience_rule",
   "audience_type",
   "bid",
   "bid_type",
   "billing_method",
   "call_to_action",
   "campaign_budget",
   "campaign_budget_mode",
   "campaign_creation_date",
   "campaign_id",
   "campaign_modify_date",
   "campaign_name",
   "campaign_objective_type",
   "campaign_operation_status",
   "conversion_bid",
   "cpv_video_duration",
   "data_source_name",
   "deep_bid_type",
   "deep_cpabid",
   "deep_external_action",
   "display_name",
   "external_action",
   "image_mode",
   "is_comment_disable",
   "keywords",
   "landing_page_url",
   "optimize_goal",
   "pacing",
   "pixel_id",
   "placement_type",
   "playable_url",
   "profile_image",
   "schedule_end_time",
   "schedule_start_time",
   "schedule_type",
   "statistic_type",
   "target_age",
   "target_android_osv",
   "target_connection_type",
   "target_device_price",
   "target_gender",
   "target_ios_osv",
   "target_languages",
   "target_operation_system",
   "video_id",
   "_fivetran_synced"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH insights AS 
    (SELECT 
        {%- for field in fields %}
        {{ get_tiktok_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    )

SELECT *,
    MAX(date) over () as last_updated,
    ad_id||'_'||date as unique_key
FROM insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-30 from {{ this }})

{% endif %}
