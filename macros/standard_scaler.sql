{% macro standard_scaler(source_table,source_columns,include_columns='*',with_mean=True) %}
{%- if with_mean!=True -%}
    {% set error_message %}
The `standard_scaler` macro only supports a 'with_mean' value of 'True' at this time.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}
{% if source_columns is not iterable or source_columns is string or source_columns is mapping %}
    {% set error_message %}
The `source_columns` parameter must contain a list of column names.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}

{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns %}
{%- endif -%}

-- generate a CTE for each source column, a single row containing the aggregates
with 
{% for source_column in source_columns %}
    {{ source_column }}_aggregates as(
        select
            avg({{ source_column }}) as avg_value,
            stddev_pop({{ source_column }}) as stddev_value
        from {{ source_table }}
    )
{% if not loop.last %}, {% endif %}
{% endfor %}

select 
    {% for column in include_columns %}
        source_table.{{ column }},
    {% endfor %}
    {% for source_column in source_columns %}
        ({{ source_column }} - {{ source_column }}_aggregates.avg_value) / {{ source_column }}_aggregates.stddev_value as {{ source_column }}_scaled
        {% if not loop.last %}, {% endif %}
    {% endfor %}
from 
    {% for source_column in source_columns %}
        {{ source_column }}_aggregates,
    {% endfor %}
    {{ source_table }} as source_table
{% endmacro %}
