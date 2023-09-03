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

{% materialization incremental, adapter='doris' %}
  {% set unique_key = config.get('unique_key', validator=validation.any[list]) %}
  {% set strategy = dbt_doris_validate_get_incremental_strategy(config) %}
  {% set full_refresh_mode = (should_full_refresh()) %}
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}
  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}
  {% set to_drop = [] %}
  {#-- append or no unique key --#}



  {% if not unique_key or strategy == 'append'  %}
        {#-- create table first --#}
        {% if existing_relation is none  %}
            {% set build_sql = doris__create_table_as(False, target_relation, sql) %}
        {% elif existing_relation.is_view or full_refresh_mode %}
            {#-- backup table is new table ,exchange table backup and old table #}
            {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
            {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
            {% do adapter.drop_relation(backup_relation) %} {#-- likes 'drop table if exists ... ' --#}
            {% set run_sql = doris__create_table_as(False, backup_relation, sql) %}
            {% call statement("run_sql") %}
                {{ run_sql }}
            {% endcall %}
            {% do exchange_relation(target_relation, backup_relation, True) %}
            {% set build_sql = "select 'hello doris'" %}
        {#-- append data --#}
        {% else %}
            {% do to_drop.append(tmp_relation) %}
            {% do run_query(create_table_as(True, tmp_relation, sql)) %}
            {% set build_sql = tmp_insert(tmp_relation, target_relation, unique_key=none) %}
        {% endif %}
  {#-- insert overwrite --#}
  {% elif strategy == 'insert_overwrite' %}
        {#-- create table first --#}
        {% if existing_relation is none  %}
            {% set build_sql = doris__create_unique_table_as(False, target_relation, sql) %}
        {#-- insert data refresh --#}
        {% elif existing_relation.is_view or full_refresh_mode %}
            {#-- backup table is new table ,exchange table backup and old table #}
            {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
            {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
            {% do adapter.drop_relation(backup_relation) %} {#-- likes 'drop table if exists ... ' --#}
            {% set run_sql = doris__create_unique_table_as(False, backup_relation, sql) %}
            {% call statement("run_sql") %}
                {{ run_sql }}
            {% endcall %}
            {% do exchange_relation(target_relation, backup_relation, True) %}
            {% set build_sql = "select 'hello doris'" %}
        {#-- append data --#}
        {% else %}
          {#-- check doris unique table  --#}
          {% if not is_unique_model(target_relation) %}
                {% do exceptions.raise_compiler_error("doris table:"~ target_relation ~ ", model must be 'UNIQUE'" ) %}
          {% endif %}
          {#-- create temp duplicate table for this incremental task  --#}
          {% do run_query(create_table_as(True, tmp_relation, sql)) %}
          {% do to_drop.append(tmp_relation) %}
          {% do adapter.expand_target_column_types(
                 from_relation=tmp_relation,
                 to_relation=target_relation) %}
          {% set build_sql = tmp_insert(tmp_relation, target_relation, unique_key=unique_key) %}
        {% endif %}
  {% else %}
          {#-- never  --#}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {#--  {% do persist_docs(target_relation, model) %}  #}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {% for rel in to_drop %}
      {% do doris__drop_relation(rel) %}
  {% endfor %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}

{% macro dbt_doris_validate_get_incremental_strategy(config) %}
  {#-- Find and validate the incremental strategy #}
  {%- set strategy = config.get('incremental_strategy') or 'insert_overwrite' -%}
  {% set invalid_strategy_msg -%}
    Invalid incremental strategy provided: {{ strategy }}
    Expected one of: 'append', 'insert_overwrite'
  {%- endset %}
  {% if strategy not in ['append', 'insert_overwrite'] %}
    {% do exceptions.raise_compiler_error(invalid_strategy_msg) %}
  {% endif %}
  {% do return (strategy) %}
{% endmacro %}
