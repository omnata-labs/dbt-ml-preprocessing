{% macro quantile_transformer(source_table,source_column,n_quantiles=10,output_distribution='uniform',ignore_implicit_zeros=False,subsample=1000,include_columns='*') %}
{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns | join(', ') %}
{%- endif -%}

with quantile_values as(
  -- Make a 0-percentile row containing the minimum value (i.e. 100% of values fall to its right)
  select 0 as quantile,(select min({{ source_column }}) from {{ source_table }}) as quantile_value
  union all
  -- generate 10 percentile values (10% increments) and for each, determine the maximum value that  
  -- will divide the dataset row counts by that percentage
  select quantile,max(case when (rownum-1)/numrows <= quantile then {{ source_column }} end) as quantile_value
  from 
  (
    select 
    row_number() over (partition by null order by null) as seq,
    seq/{{ n_quantiles-1 }} as quantile
    from table(generator(rowcount => {{ n_quantiles-1 }})) v 
    order by 1
  ) quantiles
  ,
  (
    select {{ source_column }},
      row_number() over (partition by NULL order by {{ source_column }}) as rownum,
      count(*) over (partition by NULL) as numrows
    from {{ source_table }} sample({{ subsample }} rows)
    where {{ source_column }} is not null
  ) totals
  group by quantile
  order by quantile_value
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
coalesce(y1 + ((x-x1)/(x2-x1)) * (y2-y1),0) as {{ source_column }}_TRANSFORMED
from linear_interpolation_variables
{% endmacro %}

