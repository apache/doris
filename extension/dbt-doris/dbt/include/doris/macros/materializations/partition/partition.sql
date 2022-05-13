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

{% materialization partition, default -%}

  {% set partition_by = config.get('partition_by') %}

  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_relation(this) %}
  {% set tmp_relation = make_temp_relation(target_relation) %}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {% set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') %}

  {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
  {% set backup_identifier = model['name'] + "__dbt_backup" %}

  -- the intermediate_ and backup_ relations should not already exist in the database; get_relation
  -- will return None in that case. Otherwise, we get a relation that we can drop
  -- later, before we try to use this name for the current operation. This has to happen before
  -- BEGIN, in a separate transaction
  {% set preexisting_intermediate_relation = adapter.get_relation(identifier=tmp_identifier,
                                                                  schema=schema,
                                                                  database=database) %}
  {% set preexisting_backup_relation = adapter.get_relation(identifier=backup_identifier,
                                                            schema=schema,
                                                            database=database) %}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}

  -- `BEGIN` happens here:
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}

  {# -- first check whether we want to full refresh for source view or config reasons #}
  {% set trigger_full_refresh = (full_refresh_mode or existing_relation.is_view) %}

  {% if existing_relation is none %}
      {% set build_sql = create_table_as(False, target_relation, sql) %}
    {% elif trigger_full_refresh %}
      {#-- Make sure the backup doesn't exist so we don't encounter issues with the rename below #}
      {% set tmp_identifier = model['name'] + '__dbt_tmp' %}
      {% set backup_identifier = model['name'] + '__dbt_backup' %}
      {% set intermediate_relation = existing_relation.incorporate(path={"identifier": tmp_identifier}) %}
      {% set backup_relation = existing_relation.incorporate(path={"identifier": backup_identifier}) %}

      {% set build_sql = create_table_as(False, intermediate_relation, sql) %}
      {% set need_swap = true %}
      {% do to_drop.append(backup_relation) %}
  {% else %}
    {% do to_drop.append(tmp_relation) %}
    {% do run_query(create_table_as(True, tmp_relation, sql)) %}
    {% do adapter.expand_target_column_types(
             from_relation=tmp_relation,
             to_relation=target_relation) %}

    {% set distinct_partitions = get_distinct_partitions(tmp_relation, partition_by) %}
    {% do create_partitions(target_relation,distinct_partitions) %}
    {% do insert_data_to_tmp_partitions(tmp_relation,target_relation, distinct_partitions) %}
    {% set build_sql = get_partition_replace_sql(target_relation, distinct_partitions) %}

  {% endif %}

  {% call statement("main") %}
      {{ build_sql }}
  {% endcall %}

  {% if need_swap %}
      {% do adapter.rename_relation(target_relation, backup_relation) %}
      {% do adapter.rename_relation(intermediate_relation, target_relation) %}
  {% endif %}

  {% do persist_docs(target_relation, model) %}

  {% if existing_relation is none or existing_relation.is_view or should_full_refresh() %}
    {% do create_indexes(target_relation) %}
  {% endif %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  -- `COMMIT` happens here
  {% do adapter.commit() %}

  {% for rel in to_drop %}
      {% do adapter.drop_relation(rel) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
