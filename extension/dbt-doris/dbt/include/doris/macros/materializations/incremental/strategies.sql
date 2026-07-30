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

{#
    Doris strategy macros keep dbt Core's standard arg_dict keys and accept two
    adapter-specific keys:

      source_sql               compiled model SQL for a direct, single DML
      temp_relation_exists     whether temp_relation is a named source

    append, merge and insert_overwrite normally inline source_sql. The
    delete+insert strategy receives a physical staging table because both DML
    statements must read the exact same batch.
#}

{% macro doris__get_incremental_default_sql(arg_dict) %}
    {% set effective_strategy = doris__effective_incremental_strategy(
        'default',
        arg_dict.get('unique_key')
    ) %}
    {% if effective_strategy == 'merge' %}
        {{ return(doris__get_incremental_merge_sql(arg_dict)) }}
    {% endif %}
    {{ return(doris__get_incremental_append_sql(arg_dict)) }}
{% endmacro %}


{% macro doris__get_incremental_append_sql(arg_dict) %}
    {% set target_relation = arg_dict['target_relation'] %}
    {% set dest_columns = arg_dict['dest_columns'] %}
    insert into {{ target_relation }}
        ({{ doris__incremental_dest_columns_csv(dest_columns) }})
    {{ doris__incremental_source_select(arg_dict) }}
{% endmacro %}


{% macro doris__get_incremental_merge_sql(arg_dict) %}
    {# A full-row MOW Unique Key INSERT is Doris's portable 2.1+ upsert. Native
       MERGE INTO is reserved for conditional/partial 4.1+ operations. #}
    {% set target_relation = arg_dict['target_relation'] %}
    {% set dest_columns = arg_dict['dest_columns'] %}
    insert into {{ target_relation }}
        ({{ doris__incremental_dest_columns_csv(dest_columns) }})
    {{ doris__validated_unique_source_select(arg_dict) }}
{% endmacro %}


{% macro doris__get_incremental_delete_insert_sql(arg_dict) %}
    {% set target_relation = arg_dict['target_relation'] %}
    {% set dest_columns = arg_dict['dest_columns'] %}
    {# This public strategy macro must remain one statement when called with
       dbt's standard five-key arg_dict. On a Doris Unique Key target, a full-row
       INSERT has the same final-row result as delete+insert and cannot leave an
       uncommitted server transaction. The Doris materialization explicitly
       performs the staged DELETE transaction for MOR targets around this
       returned INSERT. #}
    insert into {{ target_relation }}
        ({{ doris__incremental_dest_columns_csv(dest_columns) }})
    {% if arg_dict.get('doris_transaction_managed', false) %}
        {{ doris__incremental_source_select(arg_dict) }}
    {% else %}
        {{ doris__validated_unique_source_select(arg_dict) }}
    {% endif %}
{% endmacro %}


{% macro doris__get_delete_incremental_rows_sql(arg_dict) %}
    {% set target_relation = arg_dict['target_relation'] %}
    {% set temp_relation = arg_dict['temp_relation'] %}
    {% set unique_key = doris__normalize_unique_key(arg_dict['unique_key']) %}
    delete from {{ target_relation }} DBT_INTERNAL_DEST
    using {{ temp_relation }} DBT_INTERNAL_SOURCE
    where
        {% for key in unique_key %}
        DBT_INTERNAL_DEST.{{ adapter.quote(key) }}
            <=> DBT_INTERNAL_SOURCE.{{ adapter.quote(key) }}
            {% if not loop.last %}and{% endif %}
        {% endfor %}
{% endmacro %}


{% macro doris__get_incremental_insert_overwrite_sql(arg_dict) %}
    {% set target_relation = arg_dict['target_relation'] %}
    {% set dest_columns = arg_dict['dest_columns'] %}
    {% set partition_clause = doris__overwrite_partition_clause(
        arg_dict.get('overwrite_partitions')
    ) %}
    insert overwrite table {{ target_relation }}
        {{ partition_clause }}
        ({{ doris__incremental_dest_columns_csv(dest_columns) }})
    {{ doris__incremental_source_select(arg_dict) }}
{% endmacro %}
