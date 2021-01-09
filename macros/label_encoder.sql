{% macro label_encoder(source_table,source_column, include_columns='*') %}
with distinct_values as (
    select array_agg(distinct {{ source_column }}) within group (order by {{ source_column }} asc) as all_values_array from {{ source_table }}
)
select 
{% for column in include_columns %}
{{ source_table }}.{{ column }},
{% endfor %}
array_position({{ source_column }}::variant,all_values_array) as {{ source_column }}_ENCODED
from distinct_values,{{ source_table }}
{% endmacro %}

