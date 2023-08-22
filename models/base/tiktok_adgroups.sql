{%- set selected_fields = [
    "adgroup_id",
    "campaign_id",
    "advertiser_id",
    "adgroup_name",
    "secondary_status",
    "budget",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'tiktok_raw', 'adgroups' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_tiktok_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY adgroup_id) as last_updated_at

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_at = last_updated_at
