{% macro doris__snapshot_merge_sql_update(target, source, insert_cols) -%}
    update {{ target }}, (select dbt_scd_id, dbt_change_type, dbt_valid_to from {{ source }}) as DBT_INTERNAL_SOURCE
    set {{ target }}.dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    where DBT_INTERNAL_SOURCE.dbt_scd_id = {{ target }}.dbt_scd_id
    and DBT_INTERNAL_SOURCE.dbt_change_type = 'update'
    and {{ target }}.dbt_valid_to is null
{% endmacro %}

{% macro doris__snapshot_merge_sql_insert(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    insert into {{ target }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
{% endmacro %}
