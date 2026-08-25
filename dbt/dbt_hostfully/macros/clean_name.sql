{% macro clean_name(column_name) %}
    initcap(
        case 
            when lower(trim({{ column_name }})) like '%test%' then null
            when lower(trim({{ column_name }})) = '' then null
            when length(lower(trim({{ column_name }}))) < 2 then null
            else lower(trim({{ column_name }}))
        end
    )
{% endmacro %}