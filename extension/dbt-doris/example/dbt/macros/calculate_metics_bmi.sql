{% macro calculate_metric_bmi(weight, height, height_unit, weight_unit) %}
    case
        when {{ height_unit }} = 'cm' and {{ weight_unit }} = 'kg' then
            {{ weight }} / ({{ height }} / 100.0) ^ 2
        when {{ height_unit }} = 'inch' and {{ weight_unit }} = 'pound' then
            {{ weight }} / ({{ height }} * 0.0254) ^ 2
        else
            null
    end
{% endmacro %}
