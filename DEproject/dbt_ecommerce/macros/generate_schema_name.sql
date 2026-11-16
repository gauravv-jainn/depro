{#- Use the custom schema (staging / marts) exactly as configured, instead of
    dbt's default "<target_schema>_<custom_schema>" so table names stay
    staging.stg_orders and marts.fact_orders on both DuckDB and BigQuery. -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
