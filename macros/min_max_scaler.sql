{% macro min_max_scaler(source_table,source_columns, include_columns='*') %}

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
            min({{ source_column }}) as min_value,
            max({{ source_column }}) as max_value
        from {{ source_table }}
    )
{% if not loop.last %}, {% endif %}
{% endfor %}

select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
    ({{ source_column }} - {{ source_column }}_aggregates.min_value) / ({{ source_column }}_aggregates.max_value - {{ source_column }}_aggregates.min_value) AS {{ source_column }}_scaled
    {% if not loop.last %}, {% endif %}
{% endfor %}

from  
    {% for source_column in source_columns %}
        {{ source_column }}_aggregates,
    {% endfor %}
    {{ source_table }} as source_table
{% endmacro %}
