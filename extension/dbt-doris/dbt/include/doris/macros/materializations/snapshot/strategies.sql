{% macro doris__snapshot_hash_arguments(args) -%}
    md5(concat_ws('|', {%- for arg in args -%}
        coalesce(cast({{ arg }} as char), '')
        {% if not loop.last %}, {% endif %}
    {%- endfor -%}))
{%- endmacro %}
