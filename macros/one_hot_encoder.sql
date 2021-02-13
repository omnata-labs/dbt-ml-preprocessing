{% macro one_hot_encoder(source_table, source_column, categories='auto', handle_unknown='ignore', drop_col=none, value=none) %}

    {%- if categories=='auto' -%}
        {% set category_values_query %}
            select distinct
                {{ source_column }}
            from
                {{ source_table }}
            order by 1
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
    {%- if handle_unknown!='ignore' -%}
        {% set error_message %}
    The `one_hot_encoder` macro only supports an 'handle_unknown' value of 'ignore' at this time.
        {% endset %}
        {%- do exceptions.raise_compiler_error(error_message) -%}
    {%- endif -%}
    {{ adapter.dispatch('one_hot_encoder',packages=['dbt_ml_preprocessing'])(source_table, source_column, category_values, handle_unknown, drop_col, value) }}
{%- endmacro %}

{% macro default__one_hot_encoder(source_table, source_column, category_values, handle_unknown, drop_col, value) %}
    {% set columns = adapter.get_columns_in_relation( source_table ) %}

    with binary_output as (
    select
        {% for column in columns %}
            {%- if column.name | lower != source_column | lower %}
                {{ column.name }},
            {%- endif -%}
        {%- endfor -%}
            {% if drop_col is none or not drop_col%}
                {{ source_column }},
            {%- endif -%}
        {%- for category in category_values -%}
            {% set no_whitespace_column_name = category | replace( " ", "_") -%}
                {%- if value is not none and value | lower  in columns | lower %}
                    case when {{ source_column }} = '{{ category }}' then {{ value }} else null end as is_{{ source_column }}_{{ no_whitespace_column_name }}
                {% else %}
                    case when {{ source_column }} = '{{ category }}' then 1 else 0 end as is_{{ source_column }}_{{ no_whitespace_column_name }}
                {%- endif -%}
            {%- if not loop.last %},{% endif -%}
        {% endfor %}
    from {{ source_table }}
    )

    select * from binary_output
{%- endmacro %}
