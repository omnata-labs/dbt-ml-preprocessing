{% macro standard_scaler(source_table,source_column,include_columns='*',with_mean=True) %}
{%- if with_mean!=True -%}
    {% set error_message %}
The `robust_scaler` macro only supports a 'with_mean' value of 'True' at this time.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}

{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns | join(', ') %}
{%- endif -%}

with aggregates as(
    select
        avg({{ source_column }}) as avg_value,
        stddev_pop({{ source_column }}) as stddev_value
    from {{ source_table }}
)
select 
    {{include_columns}},
    ({{ source_column }} - avg_value) / stddev_value as {{ source_column }}_scaled
from aggregates,{{ source_table }}
{% endmacro %}
