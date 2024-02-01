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

{%- materialization view, adapter='doris' -%}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='view') -%}
  {%- set intermediate_relation =  make_intermediate_relation(target_relation) -%}
  {%- set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) -%}


  {{ drop_relation_if_exists(intermediate_relation) }}


  {% if existing_relation is not none %}
  --todo: exchange
    {% call statement('main_test') -%}
      {{ get_create_view_as_sql(intermediate_relation, sql) }}
    {%- endcall %}
    {{ drop_relation_if_exists(intermediate_relation) }}
    {{ drop_relation_if_exists(target_relation) }}
    {% call statement('main') -%}
      {{ get_create_view_as_sql(target_relation, sql) }}
    {%- endcall %}
    {# {{ adapter.rename_relation(intermediate_relation, target_relation) }} #}
  {% else %}
    {% call statement('main') -%}
      {{ get_create_view_as_sql(target_relation, sql) }}
    {%- endcall %}
  {% endif %}


  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization -%}
