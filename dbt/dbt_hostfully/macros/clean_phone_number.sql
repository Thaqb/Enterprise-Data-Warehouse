{% macro clean_phone_number(column_name) %}
    case 
        when trim({{ column_name }}) = '' then null
        when regexp_contains(trim({{ column_name }}), r'[a-zA-Z]') then null
        when starts_with(trim({{ column_name }}), '+2020') then REGEXP_REPLACE(trim({{ column_name }}), r'^\+2020', '+20') 
        when length(trim({{ column_name }})) < 8 then null
        else concat('+', regexp_replace(trim({{ column_name }}), r'\D', ''))
    end
{% endmacro %}