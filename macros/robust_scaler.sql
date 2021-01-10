{% macro robust_scaler(source_table,source_column,include_columns='*',with_centering=False,quantile_range=[25,75]) %}
{%- if with_centering!=False -%}
    {% set error_message %}
The `robust_scaler` macro only supports a 'with_centering' value of 'False' at this time.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}
{%- if quantile_range | length !=2 -%}
    {% set error_message %}
The `robust_scaler` macro only supports a 'quantile_range' value with exactly two values.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}

{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns | join(', ') %}
{%- endif -%}

with quartiles as (
  select 
  percentile_cont({{ quantile_range[0] / 100 }}) within group (order by {{ source_column }}) as first_quartile,
  percentile_cont({{ quantile_range[1] / 100 }}) within group (order by {{ source_column }}) as third_quartile
  from {{ source_table }}
  group by null
)
select 
    {{include_columns}},
    ({{ source_column }} / (third_quartile - first_quartile)) as {{ source_column }}_scaled
from quartiles,{{ source_table }}
{% endmacro %}
