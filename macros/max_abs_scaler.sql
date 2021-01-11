{% macro max_abs_scaler(source_table,source_column, include_columns='*') %}
with aggregates as (
  select max(abs({{ source_column }})) as max_abs_value
  from {{ source_table }})
select 
{% for column in include_columns %}
{{ source_table }}.{{ column }},
{% endfor %}
{{ source_column }} / max_abs_value AS {{ source_column }}_scaled
from aggregates,{{ source_table }}
{% endmacro %}
