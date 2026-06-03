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
    {{ return(sql_convert_columns_in_relation(table)) }}
{%- endmacro %}

{% macro sql_convert_columns_in_relation(table) -%}
    {% set columns = [] %}
    {% for row in table %}
        {% set col_name = row['column'] %}
        {% set col_type = row['dtype'] %}
        {% do columns.append(api.Column.create(col_name, col_type)) %}
    {% endfor %}
    {{ return(columns) }}
{%- endmacro %}

{% macro doris__alter_column_type(relation, column_name, new_column_type) -%}
    {% call statement('alter_column_type') %}
        alter table {{ relation }} modify column {{ column_name }} {{ new_column_type }}
    {% endcall %}
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
            alter table {{ relation }} modify comment '{{ relation_comment }}'
        {% endcall %}
    {% endif %}
{% endmacro %}

{% macro doris__alter_column_comment(relation, column_dict) -%}
    {#-- Views do not support MODIFY COLUMN COMMENT; column comments for views
         are set at CREATE VIEW time via column definitions --#}
    {% if relation.type != 'view' %}
        {% for column_name, column_comment in column_dict.items() %}
            {% call statement('alter_column_comment') %}
                alter table {{ relation }} modify column `{{ column_name }}` comment '{{ column_comment }}'
            {% endcall %}
        {% endfor %}
    {% endif %}
{% endmacro %}
