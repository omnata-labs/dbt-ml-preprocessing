{% macro k_bins_discretizer(source_table,source_column,include_columns='*',n_bins=20,encode='ordinal',strategy='uniform') %}
{%- if encode!='ordinal' -%}
    {% set error_message %}
The `k_bins_discretizer` macro only supports an 'encode' value of 'ordinal' at this time.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}
{%- if strategy!='uniform' -%}
    {% set error_message %}
The `k_bins_discretizer` macro only supports an 'strategy' value of 'uniform' at this time.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}
with aggregates as (
  select min({{ source_column }}) as min_value,
    max({{ source_column }}) as max_value
  from {{ source_table }})
select 
{% for column in include_columns %}
{{ source_table }}.{{ column }},
{% endfor %}
{# The Snowflake implementation of width_bucket adds an extra bucket index to the end, we undo this
   to preserve the scikit-learn behaviour #}
least(width_bucket({{ source_column }},min_value,max_value,{{ n_bins }}) - 1,{{ n_bins - 1 }}) as {{ source_column }}_BINNED
from aggregates,{{ source_table }}
{% endmacro %}

