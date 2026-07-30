-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements. See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership. The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License. You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied. See the License for the
-- specific language governing permissions and limitations
-- under the License.

{% macro doris__get_columns_in_relation(relation) -%}
    {% call statement('get_columns_in_relation', fetch_result=True) %}
        select column_name  as `column`,
               column_type  as `dtype`,
               character_maximum_length as char_size,
               numeric_precision,
               numeric_scale
        from information_schema.columns
        where table_schema = '{{ relation.schema }}'
          and table_name = '{{ relation.identifier }}'
        order by ordinal_position
    {% endcall %}
    {% set table = load_result('get_columns_in_relation').table %}
    {% set columns = [] %}
    {% for row in table %}
        {% set col_name = row['column'] %}
        {% set col_type = row['dtype'] %}
        {# Preserve complex Doris types verbatim. Only split the two primitive
           parameterized types dbt itself compares structurally. #}
        {% if col_type.lower().startswith('varchar(') %}
            {% do columns.append(api.Column(
                col_name,
                'varchar',
                row['char_size'],
                none,
                none
            )) %}
        {% elif col_type.lower().startswith('decimal(') %}
            {% do columns.append(api.Column(
                col_name,
                'decimal',
                none,
                row['numeric_precision'],
                row['numeric_scale']
            )) %}
        {% else %}
            {% do columns.append(api.Column(
                col_name,
                col_type,
                none,
                none,
                none
            )) %}
        {% endif %}
    {% endfor %}
    {{ return(columns) }}
{%- endmacro %}

{% macro doris__alter_column_type(relation, column_name, new_column_type) -%}
    {% set previous_job_id = adapter.get_latest_schema_change_job_id(relation) %}
    {% call statement('alter_column_type') %}
        alter table {{ relation }} modify column
            {{ adapter.quote(column_name) }} {{ new_column_type }}
    {% endcall %}
    {% do adapter.wait_for_schema_change(relation, previous_job_id) %}
{% endmacro %}


{% macro doris__sync_column_schemas(
    on_schema_change,
    target_relation,
    schema_changes_dict
) %}
    {% set add_to_target = schema_changes_dict['source_not_in_target'] %}
    {% set remove_from_target = schema_changes_dict['target_not_in_source'] %}
    {% set new_target_types = schema_changes_dict['new_target_types'] %}

    {% if on_schema_change == 'append_new_columns' %}
        {% if add_to_target | length > 0 %}
            {% set previous_job_id = adapter.get_latest_schema_change_job_id(
                target_relation
            ) %}
            {% do alter_relation_add_remove_columns(
                target_relation,
                add_to_target,
                none
            ) %}
            {% do adapter.wait_for_schema_change(
                target_relation,
                previous_job_id
            ) %}
        {% endif %}

    {% elif on_schema_change == 'sync_all_columns' %}
        {% if add_to_target | length > 0 or remove_from_target | length > 0 %}
            {% set previous_job_id = adapter.get_latest_schema_change_job_id(
                target_relation
            ) %}
            {% do alter_relation_add_remove_columns(
                target_relation,
                add_to_target,
                remove_from_target
            ) %}
            {% do adapter.wait_for_schema_change(
                target_relation,
                previous_job_id
            ) %}
        {% endif %}

        {% for type_change in new_target_types %}
            {% do alter_column_type(
                target_relation,
                type_change['column_name'],
                type_change['new_type']
            ) %}
        {% endfor %}
    {% endif %}
{% endmacro %}

{% macro columns_and_constraints(table_type="table") %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
    {%- set raw_column_constraints = adapter.render_raw_columns_constraints(raw_columns=model['columns']) -%}
    {% for c in raw_column_constraints -%}
      {% if table_type == "table" %}
        {{ c.get_table_column_constraint() }}{{ "," if not loop.last or raw_model_constraints }}
      {% else %}
        {{ c.get_view_column_constraint() }}{{ "," if not loop.last or raw_model_constraints }}
      {% endif %}
    {% endfor %}
{% endmacro %}

{% macro doris__get_table_columns_and_constraints() -%}
  {{ return(columns_and_constraints("table")) }}
{%- endmacro %}


{% macro doris__get_view_columns_comment() -%}
  {{ return(columns_and_constraints("view")) }}
{%- endmacro %}

{% macro doris__alter_relation_comment(relation, relation_comment) -%}
    {#-- Views do not support MODIFY COMMENT, only tables do --#}
    {% if relation.type != 'view' %}
        {% call statement('alter_relation_comment') %}
            alter table {{ relation }} modify comment '{{ relation_comment | replace("\\", "\\\\") | replace("'", "\\'") }}'
        {% endcall %}
    {% endif %}
{% endmacro %}

{% macro doris__alter_column_comment(relation, column_dict) -%}
    {#-- Views do not support MODIFY COLUMN COMMENT; column comments for views
         are set at CREATE VIEW time via column definitions --#}
    {% if relation.type != 'view' %}
        {#-- dbt hands us {column_name: column_info_dict}, not {name: description}.
             Interpolating the value directly wrote the whole dict repr into the
             comment, and its embedded quotes broke the statement outright.

             The column type is deliberately omitted: Doris accepts
             `MODIFY COLUMN <col> COMMENT '<c>'`, and naming a type there is
             rejected for distribution and key columns. --#}
        {% for column_name, column_info in column_dict.items() %}
            {% set comment = (column_info.get('description') or '') if column_info is mapping else (column_info or '') %}
            {% if comment %}
                {% call statement('alter_column_comment') %}
                    alter table {{ relation }} modify column `{{ column_name }}` comment '{{ comment | replace("\\", "\\\\") | replace("'", "\\'") }}'
                {% endcall %}
            {% endif %}
        {% endfor %}
    {% endif %}
{% endmacro %}
