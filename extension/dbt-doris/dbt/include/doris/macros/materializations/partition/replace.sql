{% macro get_partition_replace_sql(relation, partitions) %}
    {% for partition in partitions %}
    {% set items = get_partition_items(partition) %}
    {% set p = ''.join(items) %}
    alter table {{ relation }} replace partition (p{{ p }}) with temporary partition (tp{{ p }}) properties (
        "strict_range" = "false"
    );
    {% endfor %}
{% endmacro %}
