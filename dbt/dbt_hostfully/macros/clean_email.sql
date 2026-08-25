{% macro clean_email(column_name) %}
    case 
        when lower(trim({{ column_name }})) = '' then null
        when lower(trim({{ column_name }})) like '%partment%' then null
        when starts_with(lower(trim({{ column_name }})), 'test') then null
        when not REGEXP_CONTAINS(lower(trim({{ column_name }})), r'^[A-Za-z0-9._%+-]{2,}@[A-Za-z0-9-]{2,}(\.[A-Za-z0-9-]{2,})+$') then null
        else lower(trim({{ column_name }}))
    end
{% endmacro %}