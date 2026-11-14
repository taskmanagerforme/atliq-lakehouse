{# Use the target schema as-is (gold / ci) for every model, no <target>_<custom> prefixing. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ target.schema }}
{%- endmacro %}
