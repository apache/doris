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

{% macro doris__create_table_as(temporary, relation, sql) -%}
    {% set sql_header = config.get('sql_header', none) %}
    {% set table = relation.include(database=False) %}
    {{ sql_header if sql_header is not none }}
    {%if temporary %}
        {{doris__drop_relation(relation)}}
    {% endif %}
    create table {{ table }}
    {{ doris__duplicate_key() }}
    {{ doris__table_comment()}}
    {{ doris__partition_by() }}
    {{ doris__distributed_by() }}
    {{ doris__properties() }} as {{ doris__table_colume_type(sql) }};

{%- endmacro %}

{% macro doris__create_unique_table_as(temporary, relation, sql) -%}
    {% set sql_header = config.get('sql_header', none) %}
    {% set table = relation.include(database=False) %}
    {{ sql_header if sql_header is not none }}
    create table {{ table }}
    {{ doris__unique_key() }}
    {{ doris__table_comment()}}
    {{ doris__partition_by() }}
    {{ doris__distributed_by() }}
    {{ doris__properties() }} as {{ doris__table_colume_type(sql) }};

{%- endmacro %}


{% macro doris__table_colume_type(sql) -%}
    {% set cols = model.get('columns') %}
    {% if cols %}
        select {{get_table_columns_and_constraints()}} from (
            {{sql}}
        ) `_table_colume_type_name`
    {% else %}
        {{sql}}
    {%- endif -%}
{%- endmacro %}