-- macros/create_table_with_column_types.sql

{% macro create_table_with_column_types(model_name) %}

{{
  config(
    materialized='table',
    unique_key='id'
  )
}}

create table {{ model_name }} (
  {%- set original_columns = source(model_name).columns %}
  {%- for column in original_columns %}
    {{ column.name }} {{ column.column_types[column.name] if column.column_types and column.name in column.column_types else column.data_type }}{% if column.tests and 'not_null' in column.tests %} not null{% endif %}{% if loop.last %}{% else %},{% endif %}
  {%- endfor %}
);

{% endmacro %}
