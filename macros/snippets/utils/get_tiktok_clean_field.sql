{%- macro get_tiktok_clean_field(table_name, column_name) -%}

    {# /* Apply to all tables */ #}

    {%- if column_name == 'date' -%}
        {{column_name}}::date as date

    {%- elif column_name == 'stat_time_day' -%}
        {{column_name}}::date as date

    {%- elif column_name == 'spend' -%}
        {{column_name}} as cost
    
    {#- /*  End  */ -#}

    {#- /* Apply to specific table */ -#}

    {%- elif table_name == 'ads' -%}
        
        {%- if column_name in ('call_to_action','status','create_time') -%}
        {{column_name}} as ad_{{column_name}}
        {%- else -%}
        {{column_name}}
        {%- endif -%}

    {%- elif table_name == 'adgroups' -%}
        
        {%- if column_name in ('budget','optimize_goal','status','create_time') -%}
        {{column_name}} as adgroup_{{column_name}}
        {%- else -%}
        {{column_name}}
        {%- endif -%}

    {%- elif table_name == 'campaigns' -%}
        
        {%- if column_name in ('budget','status','objective_type','create_time') -%}
        {{column_name}} as campaign_{{column_name}}
        {%- else -%}
        {{column_name}}
        {%- endif -%}
    
    {#- /*  End  */ -#}

    {%- else -%}
    {{column_name}}
        
    {%- endif -%}

{% endmacro -%}
