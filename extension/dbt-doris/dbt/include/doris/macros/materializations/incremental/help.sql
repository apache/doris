{% macro is_incremental() %}
    {% if not execute %}
        {{ return(False) }}
    {% else %}
        {% set relation = adapter.get_relation(this.database, this.schema, this.table) %}
        {{ return(relation is not none
                  and relation.type == 'table'
                  and model.config.materialized in ['incremental','partition']
                  and not should_full_refresh()) }}
    {% endif %}
{% endmacro %}


{% macro tmp_delete(tmp_relation, target_relation, unique_key=none, statement_name="pre_main") %}
  {% if unique_key is not none %}
    {% set unique_key_str %}
        {% for item in unique_key %} 
                {{ item }},  
        {% endfor %}
    {% endset %}
     insert into  {{ target_relation }} ( {{unique_key_str ~'`__DORIS_DELETE_SIGN__`'}})
    select  {{ unique_key_str }} 
        1 as `__DORIS_DELETE_SIGN__`
        from {{ tmp_relation }}
  {% endif %}
{%- endmacro %}


{% macro tmp_insert(tmp_relation, target_relation, unique_key=none, statement_name="main") %}
    {%- set dest_cols_csv = adapter.get_columns_in_relation(target_relation) | map(attribute='quoted') | join(', ') -%}
    insert into {{ target_relation }} ({{ dest_cols_csv }})
    (
       select {{ dest_cols_csv }}
       from {{ tmp_relation }}
    )
{%- endmacro %}

{% macro show_create( target_relation, statement_name="table_model") %}
    show create table {{ target_relation }}
{%- endmacro %}

{% macro is_unique_model( table_create_obj ) %}
    {% set create_table = table_create_obj['data'][0][1]%}
    {{ return('\nUNIQUE KEY(' in create_table  and '\nDUPLICATE KEY(' not in create_table and '\nAGGREGATE KEY(' not in create_table) }}
{%- endmacro %}
