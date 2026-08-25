{% macro initcap(col) %}
    case 
        when {{ col }} is not null then
            concat(upper(substr(trim({{ col }}), 1, 1)), lower(substr(trim({{ col }}), 2)))
        else null
    end
{% endmacro %}