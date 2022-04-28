{% macro get_distinct_partitions(relation, partition_by) %}
    {% set sql %}
    select distinct {{ ','.join(partition_by) }} from {{ relation }} order by {{ ','.join(partition_by) }}
    {% endset %}
    {{ return(run_query(sql)) }}
{% endmacro %}

{% macro get_partition_items(partition, quote) %}
    {% set items = [] %}
    {% for item in partition %}
        {% if quote %}
            {{ items.append('"{}"'.format(item+1)) }}
        {% else %}
            {{ items.append('{}'.format(item)) }}
        {% endif %}
    {% endfor %}
    {{ return(items) }}
{% endmacro %}

{% macro insert_data_to_tmp_partitions(tmp_relation, target_relation, partitions) %}
    {% for partition in partitions %}
    {% set items = get_partition_items(partition) %}
    {% set p = ''.join(items) %}
    {% call statement() %}
    insert into {{ target_relation }} temporary partition (tp{{ p }}) select * from {{ tmp_relation }} where
    {% for k,v in partition.items() %}
        {{ k }} = {{ v }}
    {% endfor %}
    {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro create_partitions(relation, partitions) %}
    {% for partition in partitions %}
    {% set items = get_partition_items(partition) %}
    {% set items_quote = get_partition_items(partition, True) %}
    {% set p = ''.join(items) %}
    {% call statement() %}
        alter table {{ relation }} drop temporary partition if exists tp{{ p }}
    {% endcall %}
    {% call statement() %}
        alter table {{ relation }} add temporary partition tp{{ p }} values less than ({{ ','.join(items_quote) }})
    {% endcall %}
    {% call statement() %}
        alter table {{ relation }} add partition if not exists p{{ p }} values less than ({{ ','.join(items_quote) }})
    {% endcall %}
    {% endfor %}
{% endmacro %}
