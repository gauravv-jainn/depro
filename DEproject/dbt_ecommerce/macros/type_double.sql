{#- 64-bit float type that works on DuckDB, Snowflake (DOUBLE) and BigQuery (FLOAT64). -#}
{% macro type_double() -%}
    {%- if target.type == 'bigquery' -%}FLOAT64{%- else -%}DOUBLE{%- endif -%}
{%- endmacro %}
