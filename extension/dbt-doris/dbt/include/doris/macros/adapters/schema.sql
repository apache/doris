{% macro doris__drop_schema(relation) -%}
    {% call statement('drop_schema') %}
    drop schema if exists {{ relation.without_identifier() }}
    {% endcall %}
{%- endmacro %}
