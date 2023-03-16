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

{% macro doris__snapshot_string_as_time(timestamp) -%}
  {%- set result = "str_to_date('" ~ timestamp ~ "', '%Y-%m-%d %T')" -%}
  {{ return(result) }}
{%- endmacro %}

{% macro doris__snapshot_merge_sql(target, source, insert_cols) -%}
  {%- set insert_cols_csv = insert_cols | join(', ') -%}
  {%- set valid_to_col = adapter.quote('dbt_valid_to') -%}

  {%- set upsert = adapter.quote(target.schema) ~ '.' ~ adapter.quote(target.table + '__snapshot_upsert') -%}

  {% call statement('create_upsert_relation') %}
    create table if not exists {{ upsert }} like {{ target }}
  {% endcall %}

  {% call statement('insert_unchanged_data') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }}
    where dbt_scd_id not in (
      select {{ source }}.dbt_scd_id from {{ source }} 
    )
  {% endcall %}

 {% call statement('insert_updated_and_deleted') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    with updates_and_deletes as (
      select
        dbt_scd_id,
        dbt_valid_to
      from {{ source }}
      where dbt_change_type IN ('update', 'delete')
    )
    select {% for column in insert_cols %}
      {%- if column == valid_to_col -%}
        updates_and_deletes.dbt_valid_to as dbt_valid_to
      {%- else -%}
        target.{{ column }} as {{ column }}
      {%- endif %} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ target }} target
    join updates_and_deletes on target.dbt_scd_id = updates_and_deletes.dbt_scd_id;
  {% endcall %}

  {% call statement('insert_new') %}
    insert into {{ upsert }} ({{ insert_cols_csv }})
    select {% for column in insert_cols -%}
      {{ column }} {%- if not loop.last %}, {%- endif %}
    {%- endfor %}
    from {{ source }}
    where {{ source }}.dbt_change_type IN ('insert');
  {% endcall %}

  {% call statement('drop_target_relation') %}
      drop table if exists {{ target }};
  {% endcall %}
  
  {% call statement('rename_upsert_relation') %}
      alter table {{ upsert }} rename {{ target.include(database=False,schema=False) }};
  {% endcall %}

  {% call statement('drop_source_relation') %}
      drop table if exists {{ source }};
  {% endcall %}

  {% do return ('select 1') %}
{% endmacro %}
