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

{% macro doris__engine() -%}
    {% set label = 'ENGINE' %}
    {% set engine = config.get('engine', 'OLAP') %}
    {{ label }} = {{ engine }}
{%- endmacro %}

{% macro doris__partition_by() -%}
  {% set cols = config.get('partition_by', validator=validation.any[list, basestring]) %}
  {% set partition_type = config.get('partition_type', 'RANGE') %}
  {% if cols is not none %}
      {%- if cols is string -%}
        {%- set cols = [cols] -%}
      {%- endif -%}
    PARTITION BY {{ partition_type }} (
      {% for col in cols %}
        {{ col }}{% if not loop.last %},{% endif %}
      {% endfor %}
    )(
        {% set init = config.get('partition_by_init', validator=validation.any[list]) %}
        {% if init is not none %}
          {% for row in init %}
            {{ row }}{% if not loop.last %},{% endif %}
          {% endfor %}
        {% endif %}
    )
  {% endif %}
{%- endmacro %}

{% macro doris__duplicate_key() -%}
  {% set cols = config.get('duplicate_key', validator=validation.any[list, basestring]) %}
  {% if cols is not none %}
      {%- if cols is string -%}
        {%- set cols = [cols] -%}
      {%- endif -%}
    DUPLICATE KEY (
      {% for item in cols %}
        {{ item }}
      {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  {% endif %}
{%- endmacro %}

{% macro doris__table_comment() -%}
  {% set description = model.get('description', "") %}
  COMMENT '{{description}}'
{%- endmacro %}

{% macro doris__unique_key() -%}
  {% set cols = config.get('unique_key', validator=validation.any[list, basestring]) %}

  {% if cols is not none %}
    {%- if cols is string -%}
      {%- set cols = [cols] -%}
    {%- endif -%}

    UNIQUE KEY (
      {% for item in cols %}
        {{ item }}
      {% if not loop.last %},{% endif %}
      {% endfor %}
    )
  {% endif %}
{%- endmacro %}

{% macro doris__distributed_by(column_names=none) -%}
  {% set engine = config.get('engine', validator=validation.any[basestring]) %}
  {% set cols = config.get('distributed_by', validator=validation.any[list, basestring]) %}
  {% if cols is none and engine in [none,'OLAP'] %}
    {% set cols = column_names %}
  {% endif %}

  {% if cols %}
      {%- if cols is string -%}
        {%- set cols = [cols] -%}
      {%- endif -%}
    DISTRIBUTED BY HASH (
      {% for item in cols %}
        {{ item }}{% if not loop.last %},{% endif %}
      {% endfor %}
    ) BUCKETS {{ config.get('buckets', validator=validation.any[int]) or 10 }}
  {% endif %}
{%- endmacro %}

{% macro doris__properties(default_properties=none) -%}
  {# Work on a new dictionary. Mutating config.get('properties') leaked
     adapter defaults into the parsed model config for later macros. #}
  {% set properties = {} %}
  {% if default_properties %}
    {% do properties.update(default_properties) %}
  {% endif %}

  {% set configured_properties = config.get('properties', validator=validation.any[dict]) %}
  {% if configured_properties %}
    {% do properties.update(configured_properties) %}
  {% endif %}

  {% set replice_num = config.get('replication_num') %}

  {% if replice_num is not none %}
    {% do properties.update({'replication_num': replice_num}) %}
  {% endif %}

  {% if properties %}
    PROPERTIES (
        {% for key, value in properties.items() %}
          "{{ key }}" = "{{ value }}"{% if not loop.last %},{% endif %}
        {% endfor %}
    )
  {% endif %}
{%- endmacro%}

{% macro doris__drop_relation(relation) -%}
  {% if relation is not none %}
    {% set relation_type = relation.type %}
    {% if not relation_type or relation_type is none %}
        {% set relation_type = 'table' %}
    {% endif %}
    {% call statement('drop_relation', auto_begin=False) %}
      drop {{ relation_type }} if exists {{ relation }}
    {% endcall %}
  {% endif %}

{%- endmacro %}

{% macro doris__truncate_relation(relation) -%}
    {% call statement('truncate_relation') %}
      truncate table {{ relation }}
    {% endcall %}
{%- endmacro %}

{% macro doris__view_query_from_show_create(show_create_sql) -%}
  {# SHOW CREATE VIEW includes an output-column list before the real AS
     delimiter. Split only on a complete AS token: identifiers such as
     ASSET_ID must not be mistaken for that delimiter. #}
  {% set parts = modules.re.split(
      '\\s+AS\\s+',
      show_create_sql,
      maxsplit=1,
      flags=modules.re.IGNORECASE
  ) %}
  {% if parts | length != 2 %}
    {% do exceptions.raise_compiler_error(
        "Could not extract the Doris view query from SHOW CREATE VIEW output."
    ) %}
  {% endif %}
  {{ return(parts[1] | trim | trim(';')) }}
{%- endmacro %}

{% macro doris__rename_relation(from_relation, to_relation) -%}
  {% call statement('drop_relation') %}
    drop {{ to_relation.type }} if exists {{ to_relation }}
  {% endcall %}
  {% call statement('rename_relation') %}
    {% if to_relation.is_view %}
    {% set results = run_query('show create view ' + from_relation.render() ) %}
    create view {{ to_relation }} as {{ doris__view_query_from_show_create(
        results[0]['Create View']
    ) }}
    {% else %}
    alter table {{ from_relation }} rename {{ to_relation.table }}
    {% endif %}
  {% endcall %}

  {% if to_relation.is_view %}
    {% call statement('rename_relation_end_drop_old') %}
      drop view if exists {{ from_relation }}
    {% endcall %}
  {% endif %}

{%- endmacro %}


{% macro exchange_relation(relation1, relation2, is_drop_r1=false) -%}

  {% if relation2.is_view %}
    {% set from_results = run_query('show create view ' + relation1.render() ) %}
    {% set to_results = run_query('show create view ' + relation2.render() ) %}
      {% call statement('exchange_view_relation') %}
        alter view {{ relation1 }} as {{ doris__view_query_from_show_create(
            to_results[0]['Create View']
        ) }}
      {% endcall %}
    {% if is_drop_r1 %}
      {% do doris__drop_relation(relation2) %}
    {% else %}
      {% call statement('exchange_view_relation') %}
        alter view {{ relation2 }} as {{ doris__view_query_from_show_create(
            from_results[0]['Create View']
        ) }}
      {% endcall %}
    {% endif %}
  {% else %}
    {% call statement('exchange_relation') %}
      ALTER TABLE {{ relation1 }} REPLACE WITH TABLE `{{ relation2.table }}` PROPERTIES('swap' = '{{not is_drop_r1}}');
    {% endcall %}
  {% endif %}

{%- endmacro %}

{% macro doris__timestimp_id() -%}
 {{ return( (modules.datetime.datetime.now() ~ "").replace('-','').replace(':','').replace('.','').replace(' ','') ) }}
{%- endmacro %}

{% macro doris__with_label() -%}
  {% set lable_suffix_id = config.get('label_id', validator=validation.any[basestring]) %}
  {% if lable_suffix_id in [none,'DEFAULT'] %}
    WITH LABEL dbt_doris_label_{{doris__timestimp_id()}}
  {% else %}
    WITH LABEL dbt_doris_label_{{ lable_suffix_id }}
  {% endif %}
{%- endmacro %}

{% macro doris__get_or_create_relation(database, schema, identifier, type) %}
  {%- set target_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}

  {% if target_relation %}
    {% do return([true, target_relation]) %}
  {% endif %}

  {%- set new_relation = api.Relation.create(
      database=none,
      schema=schema,
      identifier=identifier,
      type=type
  ) -%}
  {% do return([false, new_relation]) %}
{% endmacro %}

{% macro drop_relation_if_exists(relation) %}
  {{ doris__drop_relation(relation) }}
{% endmacro %}

{% macro create_indexes(relation) -%}
  {# Doris does not support traditional indexes; this is a no-op #}
{%- endmacro %}

{% macro catalog_source(catalog,database,table) -%}
  `{{catalog}}`.`{{database}}`.`{{table}}`
{%- endmacro %}
