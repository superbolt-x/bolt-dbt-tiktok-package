{%- set selected_fields = [
    "campaign_id",
    "advertiser_id",
    "campaign_name",
    "status",
    "budget",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'tiktok_raw', 'campaigns' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_tiktok_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY campaign_id) as last_updated_at

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_at = last_updated_at