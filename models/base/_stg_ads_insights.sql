{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, table_name = 'tiktok_raw', 'ads_insights' -%}

{%- set exclude_fields = [
   "ctr",
   "cpm",
   "cpc"
]
-%}

{%- set fields = adapter.get_columns_in_relation(source(schema_name, table_name))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH insights AS 
    (SELECT 
        {%- for field in fields if ("cost_per" not in field and "_rate" not in field) %}
        {{ get_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, table_name) }}
    )

SELECT *,
    MAX(_fivetran_synced) over () as last_updated,
    ad_id||'_'||date as unique_key
FROM insights
{% if is_incremental() -%}

  -- this filter will only be applied on an incremental run
where date >= (select max(date)-7 from {{ this }})

{% endif %}