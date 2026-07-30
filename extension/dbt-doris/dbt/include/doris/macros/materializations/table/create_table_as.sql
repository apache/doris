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

{#--
    NOTE: this macro must emit exactly one statement.

    It used to prepend `drop table if exists ...` when temporary=True, which put
    two semicolon-separated statements into a single dbt statement. The connector
    executes those in one `execute()` call and leaves unconsumed result sets
    behind, so the next statement on that connection fails with
    `2014 Commands out of sync`. Callers now drop the relation themselves.
--#}
{% macro doris__create_table_as(
    temporary,
    relation,
    sql,
    include_sql_header=true,
    sql_is_prepared=false
) -%}
    {% set sql_header = config.get('sql_header', none) %}
    {% set table = relation.include(database=False) %}
    {% set select_sql = (
        sql if sql_is_prepared else doris__table_colume_type(sql)
    ) %}
    {{ sql_header if include_sql_header and sql_header is not none }}
    create table {{ table }}
    {{ doris__duplicate_key() }}
    {{ doris__table_comment()}}
    {{ doris__partition_by() }}
    {{ doris__distributed_by() }}
    {{ doris__properties() }} as {{ select_sql }};

{%- endmacro %}

{% macro doris__create_unique_table_as(
    temporary,
    relation,
    sql,
    include_sql_header=true,
    sql_is_prepared=false
) -%}
    {% set sql_header = config.get('sql_header', none) %}
    {% set table = relation.include(database=False) %}
    {% set select_sql = (
        sql if sql_is_prepared else doris__table_colume_type(sql)
    ) %}
    {{ sql_header if include_sql_header and sql_header is not none }}
    create table {{ table }}
    {{ doris__unique_key() }}
    {{ doris__table_comment()}}
    {{ doris__partition_by() }}
    {{ doris__distributed_by() }}
    {{ doris__properties({
        'enable_unique_key_merge_on_write': 'true'
    }) }} as {{ select_sql }};

{%- endmacro %}


{#--
    Create the frozen source used by the multi-statement delete+insert strategy.

    This is deliberately not create_table_as(True, ...). Doris does not have a
    non-physical CTAS mode on the supported 2.1+ baseline, and inheriting the
    target model's partition clauses or Unique-Key-only properties can make a
    batch staging table invalid. Keep only distribution and replication here.
--#}
{% macro doris__create_incremental_staging_table(relation, source_sql) -%}
    {% set configured_properties = config.get('properties', validator=validation.any[dict]) %}
    {% set replication_num = config.get('replication_num') %}
    {% if replication_num is none and configured_properties %}
        {% set replication_num = configured_properties.get('replication_num') %}
    {% endif %}

    create table {{ relation.include(database=False) }}
    {{ doris__distributed_by() }}
    {% if replication_num is not none %}
    properties ("replication_num" = "{{ replication_num }}")
    {% endif %}
    as {{ source_sql }};
{%- endmacro %}


{#--
    Wrap the model SQL so that declared column types are applied via CAST.

    This projection lists columns explicitly, so it may only be used when dbt
    guarantees that the declared column set matches the SQL column set exactly
    -- that is, when the model contract is enforced. `assert_columns_equivalent`
    raises a contract error on any mismatch.

    Without an enforced contract, `columns:` in schema.yml is documentation and
    must not change the model result. Projecting a partial column list there
    silently dropped every undeclared column from the target table. Column
    comments are applied separately by `persist_docs`.
--#}
{% macro doris__table_colume_type(sql) -%}
    {% set contract_config = config.get('contract') %}
    {% if contract_config and contract_config.enforced %}
        {{ get_assert_columns_equivalent(sql) }}
        select {{get_table_columns_and_constraints()}} from (
            {{sql}}
        ) `_table_colume_type_name`
    {% else %}
        {{sql}}
    {%- endif -%}
{%- endmacro %}
