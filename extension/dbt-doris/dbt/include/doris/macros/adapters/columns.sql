{% macro doris__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        select column_name              as `column`,
       data_type                as 'dtype',
       character_maximum_length as char_size,
       numeric_precision,
       numeric_scale
from information_schema.columns
where table_schema = '{{ relation.schema }}'
  and table_name = '{{ relation.identifier }}'
    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}
