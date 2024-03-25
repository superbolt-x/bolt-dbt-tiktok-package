{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'supermetrics_raw', 'campaign_report_daily_region' -%}

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
        campaign_id,
        region,
        date,
        clicks as clicks,
        cost as cost,
        impressions as impressions
     FROM insights)

SELECT *,
    MAX(date) over () as last_updated,
    campaign_id||'_'||region||'_'||date as unique_key
FROM cleaned_insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-30 from {{ this }})

{% endif %}
