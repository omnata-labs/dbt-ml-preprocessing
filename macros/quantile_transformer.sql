{% macro quantile_transformer(source_table,source_column,n_quantiles=10,output_distribution='uniform',subsample=1000,include_columns='*') %}
{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns | join(', ') %}
{%- endif -%}
{{ adapter.dispatch('quantile_transformer',packages=['dbt_ml_preprocessing'])(source_table,source_column,n_quantiles,output_distribution,subsample,include_columns) }}
{% endmacro %}

{% macro default__quantile_transformer(source_table,source_column,n_quantiles,output_distribution,subsample,include_columns) %}
with quantile_values as(
  {% for quartile_index in range(n_quantiles) %}
    {% set quartile = quartile_index / (n_quantiles-1) %}
    select {{ quartile }} as quantile,percentile_cont({{ quartile }})  within group (order by {{ source_column }})as quantile_value from {{ source_table }}
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
-- prepare to apply linear interpolation formula
linear_interpolation_variables as(
  select 
    {{include_columns}},
    {{ source_column }} as x,
    (select max(b.quantile) from quantile_values b where b.quantile_value<a.{{ source_column }}) as y1,
    (select min(b.quantile) from quantile_values b where b.quantile_value>=a.{{ source_column }}) as y2,
    (select max(b.quantile_value) from quantile_values b where b.quantile_value<a.{{ source_column }}) as x1,
    (select min(b.quantile_value) from quantile_values b where b.quantile_value>=a.{{ source_column }}) as x2
  from {{ source_table }} a
  where {{ source_column }} is not null
  order by {{ source_column }}
)
select
{{include_columns}},
coalesce(y1 + ((x-x1)/(x2-x1)) * (y2-y1),0) as {{ source_column }}_transformed
from linear_interpolation_variables
{% endmacro %}

{% macro bigquery__quantile_transformer(source_table,source_column,n_quantiles,output_distribution,subsample,include_columns) %}
with quantile_values as(
  {% for quartile_index in range(n_quantiles) %}
    {% set quartile = quartile_index / (n_quantiles-1) %}
    select {{ quartile }} as quantile,percentile_cont({{ source_column }},{{ quartile }}) OVER() as quantile_value from {{ source_table }}
    {% if not loop.last %} union all {% endif %}
  {% endfor %}
),
-- prepare to apply linear interpolation formula
linear_interpolation_variables as(
  select 
    {{include_columns}},
    {{ source_column }} as x,
    (select max(b.quantile) from quantile_values b where b.quantile_value<a.{{ source_column }}) as y1,
    (select min(b.quantile) from quantile_values b where b.quantile_value>=a.{{ source_column }}) as y2,
    (select max(b.quantile_value) from quantile_values b where b.quantile_value<a.{{ source_column }}) as x1,
    (select min(b.quantile_value) from quantile_values b where b.quantile_value>=a.{{ source_column }}) as x2
  from {{ source_table }} a
  where {{ source_column }} is not null
  order by {{ source_column }}
)
select
{{include_columns}},
coalesce(y1 + ((x-x1)/(x2-x1)) * (y2-y1),0) as {{ source_column }}_transformed
from linear_interpolation_variables
{% endmacro %}
