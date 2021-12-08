{% macro one_hot_encoder(source_table, source_column, categories='auto', handle_unknown='error',include_columns='*', exclude_columns=none) %}

    {%- if categories=='auto' -%}
        {% set category_values_query %}
            select distinct
                {{ source_column }}
            from
                {{ source_table }}
            order by {{ source_column }}
        {% endset %}
        {% set results = run_query(category_values_query) %}
        {% if execute %}
            {# Return the first column #}
            {% set category_values = results.columns[0].values() %}
        {% else %}
            {% set category_values = [] %}
        {% endif %}
    {% elif categories is not iterable or categories is string or categories is mapping %}
        {% set error_message %}
    The `categories` parameter must contain a list of category values.
        {% endset %}
        {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- else -%}
        {% set category_values = categories %}
    {%- endif -%}

    {%- if handle_unknown!='ignore' and handle_unknown!='error' -%}
        {% set error_message %}
    The 'handle_unknown' parameter requires a value of either 'ignore' (when unknown value occurs, all output columns are false) or 'error' (when unknown value occurs, raise an error).
        {% endset %}
        {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}

    {%- if include_columns!='*' and exclude_columns is not none -%}
        {% set error_message %}
    If the 'exclude_columns' parameter is set, providing 'include_columns' is invalid and must be left at its default value.
        {% endset %}
        {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}

    {%- if exclude_columns is not none and (exclude_columns is not iterable or exclude_columns is string or exclude_columns is mapping) -%}
        {% set error_message %}
    The 'exclude_columns' parameter value contain a list of column names.
        {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}

    {%- if include_columns!='*' and (include_columns is not iterable or include_columns is string or include_columns is mapping) -%}
        {% set error_message %}
    The 'include_columns' parameter value must contain either the string '*' (for all columns in source), or a list of column names.
        {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}

    {% set columns = adapter.get_columns_in_relation( source_table ) %}

    {%- if include_columns=='*' and exclude_columns is none -%}
        {% set col_list = columns %}
    {%- elif include_columns !='*'-%}
        {% set col_list = include_columns %}
    {%- else -%}
        {% set col_list = [] %}
        {% for column in columns  %}
            {%- if column.name | lower not in exclude_columns | lower %}
                {% do col_list.append(column) %}
            {%- endif -%}
        {%- endfor -%}
    {%- endif -%}

    {{ adapter.dispatch('one_hot_encoder','dbt_ml_preprocessing')(source_table, source_column, category_values, handle_unknown, col_list) }}
{%- endmacro %}

{% macro default__one_hot_encoder(source_table, source_column, category_values, handle_unknown, col_list) %}

    select
        {% for column in col_list %}
            {{ column.name }},
        {%- endfor -%}
        {% for category in category_values %}
            {% set no_whitespace_column_name = category | replace( " ", "_") -%}
                {%- if handle_unknown=='ignore' %}
                    case 
                        when {{ source_column }} = '{{ category }}' then true 
                        else false
                    end as is_{{ source_column }}_{{ no_whitespace_column_name }}
                {% endif %}
                {%- if handle_unknown=='error' %}
                    case 
                        when {{ source_column }} = '{{ category }}' then true 
                        when {{ source_column }} in ('{{ category_values | join("','") }}') then false
                        else cast('Error: unknown value found and handle_unknown parameter was "error"' as boolean)
                    end as is_{{ source_column }}_{{ no_whitespace_column_name }}
                {% endif %}
            {%- if not loop.last %},{% endif -%}
        {% endfor %}
    from {{ source_table }}
{%- endmacro %}

{% macro sqlserver__one_hot_encoder(source_table, source_column, category_values, handle_unknown, col_list) %}

    select
        {% for column in col_list %}
            {{ column.name }},
        {%- endfor -%}
        {% for category in category_values %}
            {% set no_whitespace_column_name = category | replace( " ", "_") -%}
                {%- if handle_unknown=='ignore' %}
                    case 
                        when {{ source_column }} = '{{ category }}' then 1 
                        else 0
                    end as is_{{ source_column }}_{{ no_whitespace_column_name }}
                {% endif %}
                {%- if handle_unknown=='error' %}
                    case 
                        when {{ source_column }} = '{{ category }}' then 1 
                        when {{ source_column }} in ('{{ category_values | join("','") }}') then 0
                        else cast('Error: unknown value found and handle_unknown parameter was "error"' as bit)
                    end as is_{{ source_column }}_{{ no_whitespace_column_name }}
                {% endif %}
            {%- if not loop.last %},{% endif -%}
        {% endfor %}
    from {{ source_table }}
      
{%- endmacro %}

{% macro synapse__one_hot_encoder(source_table, source_column, category_values, handle_unknown, col_list) %}
    {% do return( dbt_ml_preprocessing.sqlserver__one_hot_encoder(source_table, source_column, category_values, handle_unknown, col_list)) %}
{%- endmacro %}
