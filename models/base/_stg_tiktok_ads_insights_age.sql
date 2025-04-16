{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'tiktok_raw', 'ad_report_daily_age' -%}

{%- set exclude_fields = [
   "ad_group_id",
   "ad_group_name",
   "ad_name",
   "campaign_id",
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
        ad_id,
        age,
        stat_time_day as date,
        clicks as clicks,
        spend as cost,
        impressions as impressions
     FROM insights)

SELECT *,
    MAX(date) over () as last_updated,
    ad_id||'_'||age||'_'||date as unique_key
FROM cleaned_insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-30 from {{ this }})

{% endif %}
