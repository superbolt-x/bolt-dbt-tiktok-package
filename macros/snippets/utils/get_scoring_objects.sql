{%- macro get_scoring_objects() -%}

    TRIM(SPLIT_PART(adgroup_name, '-', 2)) as audience,
    TRIM(SPLIT_PART(ad_name, '-', 2)) as visual

{%- endmacro -%}