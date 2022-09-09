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
  {% if unique_key is none %}
    {% do exceptions.raise_compiler_error("dbt-doris incremental 'unique_key' cannot be empty" ) %}
  {% endif %}

  {% set target_relation = this.incorporate(type='table') %}


  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(this) %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% if existing_relation is none %}
      {% set build_sql = doris__create_unique_table_as(False, target_relation, sql) %}
  {% elif existing_relation.is_view or should_full_refresh() %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set backup_identifier = existing_relation.identifier ~ "__dbt_backup" %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}
      {% do adapter.drop_relation(backup_relation) %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% set build_sql = doris__create_unique_table_as(False, target_relation, sql) %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
      {% set build_show_create = show_create( target_relation, statement_name="table_model") %}
        {% call statement('table_model' , fetch_result=True)  %}
            {{ build_show_create }}
        {% endcall %}
      {%- set table_create_obj = load_result('table_model') -%}
      {% if not is_unique_model(table_create_obj) %}
            {% do exceptions.raise_compiler_error("doris table:"~ target_relation ~ ", model must be 'UNIQUE'" ) %}
      {% endif %}
      {% set tmp_relation = make_temp_relation(target_relation) %}
      {% do run_query(create_table_as(True, tmp_relation, sql)) %}
      {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}
      {% set build_sql_del = tmp_delete(tmp_relation, target_relation, unique_key=unique_key, statement_name="pre_main") %}
      {% call statement("pre_main") %}
          {{ build_sql_del }}
      {% endcall %}
      {% set build_sql = tmp_insert(tmp_relation, target_relation, unique_key=unique_key) %}
  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}


  {% do persist_docs(target_relation, model) %}
  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}
  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}
  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}



