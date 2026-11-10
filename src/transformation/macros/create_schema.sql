{% macro create_schema(relation) -%}
  {{ return(adapter.dispatch('create_schema', 'dbt')(relation)) }}
{%- endmacro %}

{% macro duckdb__create_schema(relation) -%}
  {# Bỏ qua lệnh CREATE SCHEMA nếu đích đến là AWS Glue Catalog #}
  {%- if relation.database == 'glue_catalog' -%}
    {{ return('') }}
  {%- else -%}
    {%- call statement('create_schema') -%}
      create schema if not exists {{ relation.without_identifier() }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}

{% macro default__create_schema(relation) -%}
  {%- if relation.database == 'glue_catalog' -%}
    {{ return('') }}
  {%- else -%}
    {%- call statement('create_schema') -%}
      create schema if not exists {{ relation.without_identifier() }}
    {%- endcall -%}
  {%- endif -%}
{%- endmacro %}
