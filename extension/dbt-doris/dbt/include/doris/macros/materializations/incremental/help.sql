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


{% macro doris__normalize_unique_key(unique_key) %}
    {% if unique_key is none %}
        {{ return([]) }}
    {% elif unique_key is string %}
        {{ return([unique_key]) }}
    {% else %}
        {{ return(unique_key | list) }}
    {% endif %}
{% endmacro %}


{% macro doris__effective_incremental_strategy(strategy, unique_key) %}
    {% if strategy == 'default' %}
        {{ return('merge' if doris__normalize_unique_key(unique_key) else 'append') }}
    {% endif %}
    {{ return(strategy) }}
{% endmacro %}


{% macro doris__normalized_column_names(columns) %}
    {% set names = [] %}
    {% for column in columns %}
        {% do names.append(column.name | lower) %}
    {% endfor %}
    {{ return(names) }}
{% endmacro %}


{% macro doris__validate_source_unique_key_columns(source_columns, unique_key) %}
    {% set source_names = doris__normalized_column_names(source_columns) %}
    {% set missing_keys = [] %}
    {% for key in doris__normalize_unique_key(unique_key) %}
        {% if key | lower not in source_names %}
            {% do missing_keys.append(key) %}
        {% endif %}
    {% endfor %}

    {% if missing_keys %}
        {% set message -%}
Incremental model {{ model.unique_id }} does not return configured unique_key
column(s) {{ missing_keys }}. No target data has been changed.
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}
{% endmacro %}


{% macro doris__validate_unique_key_schema_changes(schema_changes, unique_key) %}
    {% set unique_keys = [] %}
    {% for key in doris__normalize_unique_key(unique_key) %}
        {% do unique_keys.append(key | lower) %}
    {% endfor %}

    {% set changed_key_types = [] %}
    {% for type_change in schema_changes['new_target_types'] %}
        {% if type_change['column_name'] | lower in unique_keys %}
            {% do changed_key_types.append(type_change['column_name']) %}
        {% endif %}
    {% endfor %}

    {% if changed_key_types %}
        {% set message -%}
Incremental model {{ model.unique_id }} changes the data type of UNIQUE KEY
column(s) {{ changed_key_types }}. Doris cannot mutate physical key columns
during an incremental run; use --full-refresh.
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}
{% endmacro %}


{% macro doris__incremental_dest_columns_csv(dest_columns) %}
    {% set quoted_columns = [] %}
    {% for column in dest_columns %}
        {% do quoted_columns.append(adapter.quote(column.name)) %}
    {% endfor %}
    {{ return(quoted_columns | join(', ')) }}
{% endmacro %}


{% macro doris__incremental_source_select(arg_dict) %}
    {% set dest_columns = arg_dict['dest_columns'] %}
    {% set source_sql = arg_dict.get('source_sql', none) %}
    {# dbt's public strategy contract contains only target_relation,
       temp_relation, unique_key, dest_columns and incremental_predicates.
       Doris's direct path adds source_sql, but packages calling this macro with
       the standard five keys must continue to read temp_relation. #}
    {% set temp_relation_exists = arg_dict.get(
        'temp_relation_exists',
        source_sql is none
    ) %}
    select
        {% for column in dest_columns -%}
        DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    from
        {% if temp_relation_exists %}
        {{ arg_dict['temp_relation'] }} DBT_INTERNAL_SOURCE
        {% else %}
        (
            {{ source_sql }}
        ) DBT_INTERNAL_SOURCE
        {% endif %}
{% endmacro %}


{#
    Make duplicate source keys fail inside the same statement as a direct Unique
    Key upsert. The source is consumed once by a windowed derived table. A
    correlated scalar subquery maps every duplicate count to two constant rows,
    which Doris rejects before publishing the INSERT.

    Do not rewrite this as two consumers of a CTE: Doris 2.1 may inline both
    consumers, evaluating volatile model SQL twice and validating a different
    batch from the one inserted.
#}
{% macro doris__validated_unique_source_select(arg_dict) %}
    {% set unique_key = doris__normalize_unique_key(arg_dict['unique_key']) %}
    {% set source_sql = arg_dict.get('source_sql', none) %}
    {% set temp_relation_exists = arg_dict.get(
        'temp_relation_exists',
        source_sql is none
    ) %}
    {% set dest_columns = arg_dict['dest_columns'] %}

    select
        {% for column in dest_columns -%}
        DBT_INTERNAL_SOURCE.{{ adapter.quote(column.name) }}{% if not loop.last %}, {% endif %}
        {%- endfor %}
    from (
        select
            {% for column in dest_columns -%}
            DBT_INTERNAL_RAW_SOURCE.{{ adapter.quote(column.name) }},
            {%- endfor %}
            if(
                count(*) over (
                    partition by
                        {% for key in unique_key -%}
                        DBT_INTERNAL_RAW_SOURCE.{{ adapter.quote(key) }}{% if not loop.last %}, {% endif %}
                        {%- endfor %}
                ) > 1,
                2,
                1
            ) as DBT_INTERNAL_UNIQUE_KEY_VALIDATION
        from (
            {% if temp_relation_exists %}
            select * from {{ arg_dict['temp_relation'] }}
            {% else %}
            {{ source_sql }}
            {% endif %}
        ) DBT_INTERNAL_RAW_SOURCE
    ) DBT_INTERNAL_SOURCE
    where (
        select DBT_INTERNAL_VALIDATION_MARKER
        from (
            select 1 as DBT_INTERNAL_VALIDATION_MARKER
            union all
            select 2 as DBT_INTERNAL_VALIDATION_MARKER
            union all
            select 2 as DBT_INTERNAL_VALIDATION_MARKER
        ) DBT_INTERNAL_DUPLICATE_KEYS
        where DBT_INTERNAL_DUPLICATE_KEYS.DBT_INTERNAL_VALIDATION_MARKER
            = DBT_INTERNAL_SOURCE.DBT_INTERNAL_UNIQUE_KEY_VALIDATION
    ) = 1
{% endmacro %}


{% macro doris__validated_unique_ctas_source_sql(
    source_sql,
    unique_key,
    source_columns
) %}
    {% set arg_dict = {
        'source_sql': source_sql,
        'temp_relation_exists': false,
        'unique_key': unique_key,
        'dest_columns': source_columns
    } %}
    {{ return(doris__validated_unique_source_select(arg_dict)) }}
{% endmacro %}


{% macro doris__assert_staged_unique_keys(temp_relation, unique_key) %}
    {% set arg_dict = {
        'temp_relation': temp_relation,
        'temp_relation_exists': true,
        'unique_key': unique_key,
        'dest_columns': adapter.get_columns_in_relation(temp_relation)
    } %}
    {% call statement('validate_incremental_unique_keys', fetch_result=true) %}
        select count(*)
        from (
            {{ doris__validated_unique_source_select(arg_dict) }}
        ) DBT_INTERNAL_VALIDATED_SOURCE
    {% endcall %}
{% endmacro %}


{% macro doris__create_incremental_schema_view(relation, source_sql) %}
    create or replace view {{ relation }} as {{ source_sql }}
{% endmacro %}


{% macro doris__schema_changes_from_columns(source_columns, target_columns) %}
    {% set source_not_in_target = diff_columns(source_columns, target_columns) %}
    {% set target_not_in_source = diff_columns(target_columns, source_columns) %}
    {% set new_target_types = diff_column_data_types(
        source_columns,
        target_columns
    ) %}
    {% set schema_changed = (
        source_not_in_target | length > 0
        or target_not_in_source | length > 0
        or new_target_types | length > 0
    ) %}

    {{ return({
        'schema_changed': schema_changed,
        'source_not_in_target': source_not_in_target,
        'target_not_in_source': target_not_in_source,
        'source_columns': source_columns,
        'target_columns': target_columns,
        'new_target_types': new_target_types
    }) }}
{% endmacro %}


{% macro doris__raise_schema_change_failure(schema_changes) %}
    {% set message -%}
The source and target schemas on incremental model {{ model.unique_id }} are
out of sync.

Source columns not in target: {{ schema_changes['source_not_in_target'] }}
Target columns not in source: {{ schema_changes['target_not_in_source'] }}
New column types: {{ schema_changes['new_target_types'] }}

Set on_schema_change to append_new_columns or sync_all_columns, update the
schema manually, or run:

    dbt run --full-refresh --select {{ model.name }}
    {%- endset %}
    {% do exceptions.raise_compiler_error(message) %}
{% endmacro %}


{% macro doris__overwrite_partition_clause(overwrite_partitions) %}
    {% if overwrite_partitions is none %}
        {{ return('') }}
    {% endif %}

    {% if overwrite_partitions is string %}
        {% set partitions = [overwrite_partitions] %}
    {% else %}
        {% set partitions = overwrite_partitions | list %}
    {% endif %}

    {% if partitions == ['*'] %}
        {{ return('partition(*)') }}
    {% endif %}

    {% set quoted_partitions = [] %}
    {% for partition in partitions %}
        {% do quoted_partitions.append(adapter.quote(partition)) %}
    {% endfor %}
    {{ return('partition(' ~ (quoted_partitions | join(', ')) ~ ')') }}
{% endmacro %}


{% macro doris__show_create_table(target_relation, statement_name='doris_incremental_show_create') %}
    {% call statement(statement_name, fetch_result=True) %}
        show create table {{ target_relation }}
    {% endcall %}
    {% set result = load_result(statement_name) %}
    {% if result is none or result['data'] | length == 0 %}
        {{ return('') }}
    {% endif %}
    {{ return(result['data'][0][1]) }}
{% endmacro %}


{% macro doris__get_table_model(target_relation) %}
    {% set create_table = doris__show_create_table(target_relation) | upper %}
    {% if 'UNIQUE KEY(' in create_table %}
        {{ return('unique') }}
    {% elif 'AGGREGATE KEY(' in create_table %}
        {{ return('aggregate') }}
    {% elif 'DUPLICATE KEY(' in create_table %}
        {{ return('duplicate') }}
    {% endif %}
    {{ return('unknown') }}
{% endmacro %}


{% macro doris__is_mow_unique_model(target_relation) %}
    {% set create_table = doris__show_create_table(
        target_relation,
        statement_name='doris_incremental_show_create_mow'
    ) | lower | replace(' ', '') | replace('\n', '') %}
    {{ return(
        'uniquekey(' in create_table
        and '"enable_unique_key_merge_on_write"="true"' in create_table
    ) }}
{% endmacro %}


{% macro doris__has_physical_sequence_column(target_relation) %}
    {% set create_table = doris__show_create_table(
        target_relation,
        statement_name='doris_incremental_show_create_sequence'
    ) | lower | replace(' ', '') | replace('\n', '') %}
    {{ return(
        '"function_column.sequence_col"=' in create_table
        or '"function_column.sequence_type"=' in create_table
    ) }}
{% endmacro %}


{% macro doris__get_unique_key_columns(target_relation) %}
    {% call statement('doris_incremental_unique_key_columns', fetch_result=True) %}
        select column_name
        from information_schema.columns
        where table_schema = '{{ target_relation.schema | replace("'", "''") }}'
          and table_name = '{{ target_relation.identifier | replace("'", "''") }}'
          and column_key = 'UNI'
        order by ordinal_position
    {% endcall %}
    {% set result = load_result('doris_incremental_unique_key_columns') %}
    {% set columns = [] %}
    {% for row in result['data'] %}
        {% do columns.append(row[0]) %}
    {% endfor %}
    {{ return(columns) }}
{% endmacro %}


{% macro doris__validate_incremental_target(strategy, target_relation, unique_key) %}
    {% set table_model = doris__get_table_model(target_relation) %}

    {% if strategy == 'append' and table_model != 'duplicate' %}
        {% set message -%}
Doris incremental strategy 'append' requires a DUPLICATE KEY target, but
{{ target_relation }} is {{ table_model | upper }}. Rebuild the model with:

    dbt run --full-refresh --select {{ model.name }}
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}

    {% if strategy in ['merge', 'delete+insert'] %}
        {% if table_model != 'unique' %}
            {% set message -%}
Doris incremental strategy '{{ strategy }}' requires a UNIQUE KEY target, but
{{ target_relation }} is {{ table_model | upper }}. Rebuild the model with:

    dbt run --full-refresh --select {{ model.name }}
            {%- endset %}
            {% do exceptions.raise_compiler_error(message) %}
        {% endif %}

        {% set configured_keys = [] %}
        {% for key in doris__normalize_unique_key(unique_key) %}
            {% do configured_keys.append(key | lower) %}
        {% endfor %}
        {% set physical_keys = [] %}
        {% for key in doris__get_unique_key_columns(target_relation) %}
            {% do physical_keys.append(key | lower) %}
        {% endfor %}

        {% if configured_keys != physical_keys %}
            {% set message -%}
Doris incremental strategy '{{ strategy }}' configured unique_key
{{ configured_keys }}, but {{ target_relation }} uses physical UNIQUE KEY
{{ physical_keys }}. Rebuild it with:

    dbt run --full-refresh --select {{ model.name }}
            {%- endset %}
            {% do exceptions.raise_compiler_error(message) %}
        {% endif %}
    {% endif %}

    {% if strategy == 'merge' and not doris__is_mow_unique_model(target_relation) %}
        {% set message -%}
Doris incremental strategy 'merge' requires a Merge-on-Write UNIQUE KEY target.
{{ target_relation }} is not Merge-on-Write. Rebuild it with:

    dbt run --full-refresh --select {{ model.name }}
        {%- endset %}
        {% do exceptions.raise_compiler_error(message) %}
    {% endif %}
{% endmacro %}


{# Backwards-compatible helpers retained for packages that called them directly. #}
{% macro tmp_insert(tmp_relation, target_relation, unique_key=none, statement_name='main') %}
    {% set dest_columns = adapter.get_columns_in_relation(target_relation) %}
    {% set arg_dict = {
        'temp_relation': tmp_relation,
        'temp_relation_exists': true,
        'dest_columns': dest_columns
    } %}
    insert into {{ target_relation }}
        ({{ doris__incremental_dest_columns_csv(dest_columns) }})
    {{ doris__incremental_source_select(arg_dict) }}
{% endmacro %}


{% macro tmp_delete(tmp_relation, target_relation, unique_key=none, statement_name='pre_main') %}
    {% set keys = doris__normalize_unique_key(unique_key) %}
    delete from {{ target_relation }} DBT_INTERNAL_DEST
    using {{ tmp_relation }} DBT_INTERNAL_SOURCE
    where
        {% for key in keys %}
        DBT_INTERNAL_DEST.{{ adapter.quote(key) }}
            <=> DBT_INTERNAL_SOURCE.{{ adapter.quote(key) }}
            {% if not loop.last %}and{% endif %}
        {% endfor %}
{% endmacro %}


{% macro show_create(target_relation, statement_name='table_model') %}
    show create table {{ target_relation }}
{% endmacro %}


{% macro is_unique_model(target_relation) %}
    {{ return(doris__get_table_model(target_relation) == 'unique') }}
{% endmacro %}
