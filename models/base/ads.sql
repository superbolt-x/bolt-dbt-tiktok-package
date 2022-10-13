{%- set selected_fields = [
    "ad_id",
    "adgroup_id",
    "advertiser_id",
    "ad_name",
    "status",
    "ad_text",
    "landing_page_url",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'tiktok_raw', 'ads' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY ad_id) as last_updated_at

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_at = last_updated_at