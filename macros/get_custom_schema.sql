{% macro generate_schema_name(custom_schema_name, node) -%}

   {{ "cp_dbt_standard_package." ~ custom_schema_name | trim }}

{%- endmacro %}