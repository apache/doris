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

{% macro doris__snapshot_merge_sql_update(target, source, insert_cols) -%}
    update {{ target }}, (select dbt_scd_id, dbt_change_type, dbt_valid_to from {{ source }}) as DBT_INTERNAL_SOURCE
    set {{ target }}.dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to
    where DBT_INTERNAL_SOURCE.dbt_scd_id = {{ target }}.dbt_scd_id
    and DBT_INTERNAL_SOURCE.dbt_change_type = 'update'
    and {{ target }}.dbt_valid_to is null
{% endmacro %}

{% macro doris__snapshot_merge_sql_insert(target, source, insert_cols) -%}
    {%- set insert_cols_csv = insert_cols | join(', ') -%}

    insert into {{ target }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
        DBT_INTERNAL_SOURCE.{{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }} as DBT_INTERNAL_SOURCE
    where DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
{% endmacro %}
