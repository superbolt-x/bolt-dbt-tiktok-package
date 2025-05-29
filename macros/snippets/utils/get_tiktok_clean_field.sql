{%- macro get_tiktok_clean_field(table_name, column_name) -%}

    {# /* Apply to all tables */ #}

    {%- if column_name == 'date' -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}}::date END as date

    {%- elif column_name == 'stat_time_day' -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}}::date END as date

    {%- elif column_name == 'spend' -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END as cost
    
    {#- /*  End  */ -#}

    {#- /* Apply to specific table */ -#}

    {%- elif table_name == 'ads' -%}
        
        {%- if column_name in ('call_to_action','status','create_time') -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END as ad_{{column_name}}
        {%- else -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END
        {%- endif -%}

    {%- elif table_name == 'adgroups' -%}
        
        {%- if column_name in ('budget','optimize_goal','status','create_time') -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END as adgroup_{{column_name}}
        {%- else -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END
        {%- endif -%}

    {%- elif table_name == 'campaigns' -%}
        
        {%- if column_name in ('budget','status','objective_type','create_time') -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END as campaign_{{column_name}}
        {%- else -%}
        CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END
        {%- endif -%}
    
    {#- /*  End  */ -#}

    {%- else -%}
    CASE WHEN {{column_name}}::text = '-' THEN NULL ELSE {{column_name}} END
        
    {%- endif -%}

{% endmacro -%}