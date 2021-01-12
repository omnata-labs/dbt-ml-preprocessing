{% macro min_max_scaler(source_table,source_column, include_columns='*') %}
with aggregates as (
  select min({{ source_column }}) as min_value,
    max({{ source_column }}) as max_value
  from {{ source_table }})
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
({{ source_column }} - min_value) / (max_value - min_value) AS {{ source_column }}_scaled
from aggregates,{{ source_table }} as source_table
{% endmacro %}
