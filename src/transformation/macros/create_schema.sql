{% macro create_schema(relation) -%}
  {{ return(adapter.dispatch('create_schema', 'dbt')(relation)) }}
{%- endmacro %}

{% macro duckdb__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}
{%- endmacro %}

{% macro default__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}
{%- endmacro %}
