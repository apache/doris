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
  {% set target_relation = this.incorporate(type='table') %}
  {% set existing_relation = load_cached_relation(this) %}
  {% set temp_relation = make_temp_relation(target_relation) %}
  {% set intermediate_relation = make_intermediate_relation(target_relation) %}
  {# A failed view -> table replacement can leave the old object at dbt's
     backup name while the canonical target is absent. Restore it before stale
     helper cleanup, so a retry can never delete the only good copy. #}
  {% set recovery_backup_relation = load_cached_relation(
      make_backup_relation(target_relation, 'table')
  ) %}
  {% if existing_relation is none and recovery_backup_relation is not none %}
      {% set recovery_target_relation = target_relation.incorporate(
          type=recovery_backup_relation.type
      ) %}
      {% do adapter.rename_relation(
          recovery_backup_relation,
          recovery_target_relation
      ) %}
      {% set existing_relation = load_cached_relation(
          recovery_target_relation
      ) %}
  {% endif %}

  {% set backup_relation_type = (
      'table' if existing_relation is none else existing_relation.type
  ) %}
  {% set backup_relation = make_backup_relation(
      target_relation,
      backup_relation_type
  ) %}

  {% set unique_key = config.get('unique_key') %}
  {% set strategy = dbt_doris_validate_get_incremental_strategy(config) %}
  {% set effective_strategy = doris__effective_incremental_strategy(
      strategy,
      unique_key
  ) %}
  {# Resolve the public dbt strategy before hooks or writes, so a missing
     custom macro and an unsupported built-in fail early. #}
  {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(
      context,
      strategy
  ) %}
  {% set full_refresh_mode = (
      should_full_refresh()
      or (existing_relation is not none and existing_relation.type != 'table')
  ) %}
  {% set on_schema_change = incremental_validate_on_schema_change(
      config.get('on_schema_change'),
      default='ignore'
  ) %}
  {% set incremental_predicates = (
      config.get('predicates', none)
      or config.get('incremental_predicates', none)
  ) %}
  {% set overwrite_partitions = config.get('overwrite_partitions', none) %}
  {% set grant_config = config.get('grants') %}

  {% set preexisting_temp_relation = load_cached_relation(temp_relation) %}
  {% set preexisting_intermediate_relation = load_cached_relation(
      intermediate_relation
  ) %}
  {% set preexisting_backup_relation = load_cached_relation(backup_relation) %}
  {{ drop_relation_if_exists(preexisting_temp_relation) }}
  {{ drop_relation_if_exists(preexisting_intermediate_relation) }}
  {{ drop_relation_if_exists(preexisting_backup_relation) }}

  {% if existing_relation is not none and not full_refresh_mode %}
      {% do doris__validate_incremental_target(
          effective_strategy,
          target_relation,
          unique_key
      ) %}
  {% endif %}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set to_drop = [] %}
  {% set need_swap = false %}
  {% set delete_insert_transaction = false %}
  {% set sql_header = config.get('sql_header', none) %}
  {# Execute the header once, before metadata inspection and model SQL. Embedding
     it in the empty-schema query loses cursor.description when the header is a
     SET statement, while repeating it in helper/view/DML statements is not the
     dbt sql_header contract. #}
  {% if sql_header is not none %}
      {% do run_query(sql_header) %}
  {% endif %}
  {% set source_sql = doris__table_colume_type(sql) %}

  {# A MOW Unique Key INSERT already has the exact final-row semantics of
     delete+insert when predicates and partial updates are absent. Avoid the
     physical batch and the Doris 3.0 transaction requirement in that case.
     Existing MOR Unique targets retain the real two-statement strategy. #}
  {% set execution_strategy = effective_strategy %}
  {% set target_has_sequence_column = false %}
  {% if (
      existing_relation is not none
      and not full_refresh_mode
      and effective_strategy == 'delete+insert'
  ) %}
      {% set target_has_sequence_column =
          doris__has_physical_sequence_column(target_relation) %}
  {% endif %}
  {% if target_has_sequence_column %}
      {% do exceptions.raise_compiler_error(
          "Doris incremental strategy 'delete+insert' is unsafe for a "
          ~ "Unique Key target with a physical Sequence Column on "
          ~ model.unique_id ~ ": the DELETE tombstone can retain the old "
          ~ "sequence and suppress the replacement INSERT. Use strategy='merge' "
          ~ "to preserve Doris Sequence semantics, or rebuild without Sequence."
      ) %}
  {% endif %}
  {% if (
      existing_relation is not none
      and not full_refresh_mode
      and effective_strategy == 'delete+insert'
      and doris__is_mow_unique_model(target_relation)
      and not target_has_sequence_column
  ) %}
      {% set execution_strategy = 'merge' %}
      {% set strategy_sql_macro_func = adapter.get_incremental_strategy_macro(
          context,
          'merge'
      ) %}
      {% do log(
          "Routing Doris delete+insert to one-statement MOW Unique Key upsert "
          ~ "for " ~ target_relation,
          info=false
      ) %}
  {% endif %}

  {# Validate configured keys against the query metadata before CTAS, DELETE,
     INSERT, or target schema mutation. This is a zero-row metadata query. #}
  {% set source_columns = none %}
  {% if effective_strategy in ['merge', 'delete+insert'] %}
      {% set source_columns = get_column_schema_from_query(
          source_sql
      ) %}
      {% do doris__validate_source_unique_key_columns(
          source_columns,
          unique_key
      ) %}
  {% endif %}

  {% if (
      execution_strategy == 'delete+insert'
      and (
          (existing_relation is not none and not full_refresh_mode)
          or (
              (config.get('properties', none) or {}).get(
                  'enable_unique_key_merge_on_write',
                  none
              ) | string | lower == 'false'
          )
      )
      and not adapter.supports_transactional_delete_insert()
  ) %}
      {% do exceptions.raise_compiler_error(
          "Doris incremental strategy 'delete+insert' requires Doris 3.0+ "
          ~ "for transactional DELETE plus INSERT on " ~ model.unique_id
          ~ ". Use a MOW Unique Key table without a Sequence Column and "
          ~ "strategy='merge', or upgrade Doris."
      ) %}
  {% endif %}

  {% if existing_relation is none %}
      {% set build_sql = doris__get_incremental_create_table_as_sql(
          effective_strategy,
          target_relation,
          source_sql,
          source_columns
      ) %}
      {% set relation_for_indexes = target_relation %}

  {% elif full_refresh_mode %}
      {% set build_sql = doris__get_incremental_create_table_as_sql(
          effective_strategy,
          intermediate_relation,
          source_sql,
          source_columns
      ) %}
      {% set relation_for_indexes = intermediate_relation %}
      {% set need_swap = true %}

  {% else %}
      {% set source_relation = none %}
      {% set temp_relation_exists = false %}
      {% set dest_columns = none %}

      {# Multi-statement/custom strategies and schema-changing runs need a
         frozen batch. Ordinary built-ins use only a metadata view and execute
         one direct DML against inline source_sql. #}
      {% set needs_physical_staging = (
          execution_strategy not in ['append', 'merge', 'insert_overwrite']
          or on_schema_change != 'ignore'
      ) %}
      {% if execution_strategy == 'delete+insert' %}
          {% set delete_insert_transaction = true %}
      {% endif %}

      {% if needs_physical_staging %}
          {% do run_query(doris__create_incremental_staging_table(
              temp_relation,
              source_sql
          )) %}
          {% set source_relation = temp_relation %}
          {% set temp_relation_exists = true %}
          {% do to_drop.append(source_relation) %}

      {% else %}
          {# A logical view stores no batch data. It gives Doris/dbt exact
             VARCHAR lengths for Core-compatible type widening and preserves
             dbt's standard five-key strategy contract through the DML. #}
          {% set source_relation = temp_relation.incorporate(type='view') %}
          {% do run_query(doris__create_incremental_schema_view(
              source_relation,
              source_sql
          )) %}
          {% set temp_relation_exists = true %}
          {% do to_drop.append(source_relation) %}
      {% endif %}

      {% if source_relation is not none %}
          {% if on_schema_change != 'ignore' %}
              {% set schema_changes = check_for_schema_changes(
                  source_relation,
                  existing_relation
              ) %}
              {% if effective_strategy in ['merge', 'delete+insert'] %}
                  {% do doris__validate_unique_key_schema_changes(
                      schema_changes,
                      unique_key
                  ) %}
              {% endif %}

          {% endif %}

          {% set contract_config = config.get('contract') %}
          {% if (
              source_relation is not none
              and (not contract_config or not contract_config.enforced)
          ) %}
              {% do adapter.expand_target_column_types(
                  from_relation=source_relation,
                  to_relation=target_relation
              ) %}
          {% endif %}

          {# Re-read after widening: dbt Core does not treat an automatically
             widened string column as an on_schema_change mismatch. #}
          {% if (
              on_schema_change != 'ignore'
              and source_relation is not none
          ) %}
              {% set schema_changes = check_for_schema_changes(
                  source_relation,
                  existing_relation
              ) %}
          {% endif %}

          {% if on_schema_change != 'ignore' %}
              {% if (
                  on_schema_change == 'fail'
                  and schema_changes['schema_changed']
              ) %}
                  {# Remove the frozen physical stage before raising. #}
                  {% if source_relation is not none %}
                      {% do adapter.drop_relation(source_relation) %}
                  {% endif %}
                  {% do doris__raise_schema_change_failure(schema_changes) %}
              {% endif %}

              {% if schema_changes['schema_changed'] %}
                  {% do sync_column_schemas(
                      on_schema_change,
                      target_relation,
                      schema_changes
                  ) %}
              {% endif %}
              {% set dest_columns = schema_changes['source_columns'] %}
          {% endif %}
      {% endif %}

      {% if not dest_columns %}
          {% set dest_columns = adapter.get_columns_in_relation(existing_relation) %}
      {% endif %}

      {% set strategy_arg_dict = {
          'target_relation': target_relation,
          'temp_relation': temp_relation,
          'unique_key': unique_key,
          'dest_columns': dest_columns,
          'incremental_predicates': incremental_predicates,
          'source_sql': source_sql,
          'temp_relation_exists': temp_relation_exists,
          'overwrite_partitions': overwrite_partitions,
          'doris_transaction_managed': delete_insert_transaction
      } %}
      {% set build_sql = strategy_sql_macro_func(strategy_arg_dict) %}
      {% if delete_insert_transaction %}
          {% set delete_sql = doris__get_delete_incremental_rows_sql(
              strategy_arg_dict
          ) %}
      {% endif %}
  {% endif %}

  {% if delete_insert_transaction %}
      {% do doris__assert_staged_unique_keys(temp_relation, unique_key) %}
      {% call statement(
          'begin_delete_insert_transaction',
          auto_begin=false
      ) %}
          begin
      {% endcall %}
      {% call statement('delete_incremental_rows', auto_begin=false) %}
          {{ delete_sql }}
      {% endcall %}
  {% endif %}

  {% call statement('main') %}
      {{ build_sql }}
  {% endcall %}

  {# DorisConnectionManager.commit intentionally manages only dbt's logical
     flag. Real delete+insert opened a server transaction, so close it before
     any hook, SHOW, DDL, grant, docs, or staging cleanup statement. A failed
     DELETE/INSERT/COMMIT is rolled back by the connection exception handler. #}
  {% if delete_insert_transaction %}
      {% call statement(
          'commit_delete_insert_transaction',
          auto_begin=false
      ) %}
          commit
      {% endcall %}
  {% endif %}

  {% if existing_relation is none or full_refresh_mode %}
      {% do create_indexes(relation_for_indexes) %}
  {% endif %}

  {% if need_swap %}
      {% if existing_relation.type == 'table' %}
          {# swap=true leaves the old target online under intermediate_relation
             until all post-processing succeeds and cleanup runs. #}
          {% do exchange_relation(
              target_relation,
              intermediate_relation,
              false
          ) %}
          {% do to_drop.append(intermediate_relation) %}
      {% else %}
          {% do adapter.rename_relation(existing_relation, backup_relation) %}
          {% do adapter.rename_relation(intermediate_relation, target_relation) %}
          {% do to_drop.append(backup_relation) %}
      {% endif %}
  {% endif %}

  {% set should_revoke = should_revoke(
      existing_relation,
      full_refresh_mode=full_refresh_mode
  ) %}
  {% do apply_grants(
      target_relation,
      grant_config,
      should_revoke=should_revoke
  ) %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {% do adapter.commit() %}

  {% for relation in to_drop %}
      {% do adapter.drop_relation(relation) %}
  {% endfor %}

  {{ run_hooks(post_hooks, inside_transaction=False) }}
  {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}


{% macro doris__get_incremental_create_table_as_sql(
    strategy,
    relation,
    sql,
    source_columns=none
) %}
    {% if strategy in ['merge', 'delete+insert'] %}
        {% set validated_sql = doris__validated_unique_ctas_source_sql(
            sql,
            config.get('unique_key'),
            source_columns
        ) %}
        {{ return(doris__create_unique_table_as(
            false,
            relation,
            validated_sql,
            false,
            true
        )) }}
    {% endif %}
    {{ return(doris__create_table_as(
        false,
        relation,
        sql,
        false,
        true
    )) }}
{% endmacro %}


{% macro dbt_doris_validate_get_incremental_strategy(config) %}
    {% set unique_key = config.get('unique_key') %}
    {% set strategy = config.get('incremental_strategy') or 'default' %}

    {% if config.get('grants', none) %}
        {% do exceptions.raise_compiler_error(
            "The dbt 'grants' config is not implemented by dbt-doris yet. "
            ~ "Remove it from incremental model " ~ model.unique_id
            ~ " and manage Doris privileges separately."
        ) %}
    {% endif %}

    {% if config.get('sequence_col', none) %}
        {% do exceptions.raise_compiler_error(
            "dbt-doris does not implement the bare 'sequence_col' config. "
            ~ "Use Doris table property 'function_column.sequence_col' on "
            ~ model.unique_id ~ " instead."
        ) %}
    {% endif %}

    {# dbt maps '+' to '_' when resolving a strategy macro. Without this guard,
       the non-standard alias silently resolves Doris's built-in macro while
       bypassing dbt's built-in strategy validation. #}
    {% if strategy == 'delete_insert' %}
        {% do exceptions.raise_compiler_error(
            "Incremental strategy 'delete_insert' is not a dbt strategy name. "
            ~ "Use 'delete+insert' on model " ~ model.unique_id
        ) %}
    {% endif %}

    {% set effective_strategy = doris__effective_incremental_strategy(
        strategy,
        unique_key
    ) %}
    {% set properties = config.get('properties', none) or {} %}
    {% set normalized_property_names = [] %}
    {% for property_name in properties.keys() %}
        {% do normalized_property_names.append(property_name | lower) %}
    {% endfor %}
    {% if (
        effective_strategy == 'delete+insert'
        and (
            'function_column.sequence_col' in normalized_property_names
            or 'function_column.sequence_type' in normalized_property_names
        )
    ) %}
        {% do exceptions.raise_compiler_error(
            "Incremental strategy 'delete+insert' cannot create a Doris "
            ~ "Sequence Column target on " ~ model.unique_id
            ~ ". Use strategy='merge' to preserve Sequence semantics."
        ) %}
    {% endif %}

    {% set configured_mow = properties.get(
        'enable_unique_key_merge_on_write',
        none
    ) %}
    {% if (
        effective_strategy == 'merge'
        and configured_mow is not none
        and configured_mow | string | lower == 'false'
    ) %}
        {% do exceptions.raise_compiler_error(
            "Incremental strategy 'merge' requires "
            ~ "properties.enable_unique_key_merge_on_write=true on model "
            ~ model.unique_id ~ ". Use strategy='delete+insert' for a "
            ~ "Merge-on-Read Unique Key table."
        ) %}
    {% endif %}

    {% set normalized_unique_key = doris__normalize_unique_key(unique_key) %}
    {% if (
        effective_strategy in ['merge', 'delete+insert']
        and not normalized_unique_key
    ) %}
        {% set message -%}
Incremental strategy '{{ effective_strategy }}' requires a 'unique_key' config on model
{{ model.unique_id }}.

Add a key, for example:
    {{ '{{' }} config(
        materialized='incremental',
        incremental_strategy='{{ effective_strategy }}',
        unique_key=['id']
    ) {{ '}}' }}
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}

    {% if effective_strategy in ['merge', 'delete+insert'] %}
        {% for key in normalized_unique_key %}
            {% if key is not string or not modules.re.fullmatch(
                '[A-Za-z_][A-Za-z0-9_]*',
                key
            ) %}
                {% set message -%}
Invalid unique_key {{ key }} on model {{ model.unique_id }}. Doris table keys
must be unquoted column names containing only letters, digits, and underscores.
                {%- endset %}
                {% do exceptions.raise_compiler_error(message) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% set overwrite_partitions = config.get('overwrite_partitions', none) %}
    {% if (
        overwrite_partitions is not none
        and effective_strategy != 'insert_overwrite'
    ) %}
        {% set message -%}
Config 'overwrite_partitions' is only valid with incremental strategy
'insert_overwrite' on model {{ model.unique_id }}.
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}

    {% if overwrite_partitions is not none %}
        {% if overwrite_partitions is string %}
            {% set partitions = [overwrite_partitions] %}
        {% else %}
            {% set partitions = overwrite_partitions | list %}
        {% endif %}

        {% if not partitions %}
            {% do exceptions.raise_compiler_error(
                "Config 'overwrite_partitions' must not be empty on model "
                ~ model.unique_id
            ) %}
        {% endif %}
        {% if '*' in partitions and partitions != ['*'] %}
            {% do exceptions.raise_compiler_error(
                "Dynamic partition '*' cannot be combined with named "
                "overwrite_partitions on model " ~ model.unique_id
            ) %}
        {% endif %}
        {% if config.get('partition_by', none) is none %}
            {% do exceptions.raise_compiler_error(
                "Config 'overwrite_partitions' requires a partitioned Doris "
                "target via 'partition_by' on model " ~ model.unique_id
            ) %}
        {% endif %}
        {% for partition in partitions %}
            {% if partition != '*' and (
                partition is not string
                or not modules.re.fullmatch(
                    '[A-Za-z_][A-Za-z0-9_]*',
                    partition
                )
            ) %}
                {% do exceptions.raise_compiler_error(
                    "Unsafe Doris partition name '" ~ partition
                    ~ "' in overwrite_partitions on model " ~ model.unique_id
                ) %}
            {% endif %}
        {% endfor %}
    {% endif %}

    {% set incremental_predicates = (
        config.get('predicates', none)
        or config.get('incremental_predicates', none)
    ) %}
    {% if incremental_predicates and effective_strategy in [
        'append',
        'merge',
        'delete+insert',
        'insert_overwrite'
    ] %}
        {% set message -%}
Config 'incremental_predicates' is not supported by Doris strategy
'{{ effective_strategy }}'. Conditional target filtering requires the Doris
4.1+ native MERGE INTO path, which is not enabled yet.
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}

    {% if effective_strategy == 'merge' and (
        config.get('merge_update_columns', none)
        or config.get('merge_exclude_columns', none)
    ) %}
        {% set message -%}
Doris strategy 'merge' currently performs a full-row Unique Key upsert.
'merge_update_columns' and 'merge_exclude_columns' require the Doris 4.1+
native MERGE INTO path, which is not enabled yet.
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}

    {% set properties = config.get('properties', none) or {} %}
    {% set mow = properties.get('enable_unique_key_merge_on_write') %}
    {% if (
        effective_strategy == 'merge'
        and mow is not none
        and mow | string | lower != 'true'
    ) %}
        {% set message -%}
Doris strategy 'merge' requires
properties={'enable_unique_key_merge_on_write': 'true'} on model
{{ model.unique_id }}.
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}

    {{ return(strategy) }}
{% endmacro %}
