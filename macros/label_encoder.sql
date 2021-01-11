{% macro label_encoder(source_table,source_column, include_columns='*') %}
{{ adapter.dispatch('label_encoder',packages=['dbt_ml_preprocessing'])(source_table,source_column,include_columns) }}
{% endmacro %}

{% macro default__label_encoder(source_table,source_column,include_columns) %}
with distinct_values as (
    select array_agg(distinct {{ source_column }}) within group (order by {{ source_column }} asc) as all_values_array from {{ source_table }}
)
select 
{% for column in include_columns %}
{{ source_table }}.{{ column }},
{% endfor %}
array_position({{ source_column }}::variant,all_values_array) as {{ source_column }}_encoded
from distinct_values,{{ source_table }}
{% endmacro %}

{% macro bigquery__label_encoder(source_table,source_column,include_columns) %}
with distinct_values as (
    select array_agg(distinct {{ source_column }} order by {{ source_column }} asc) as all_values_array from {{ source_table }}
),
distinct_values_unnested as (
SELECT *
FROM distinct_values
CROSS JOIN UNNEST(distinct_values.all_values_array) AS element
WITH OFFSET AS offset
ORDER BY offset
)
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
(select offset from distinct_values_unnested where element={{ source_column }}) as {{ source_column }}_encoded
from distinct_values,{{ source_table }} as source_table
{% endmacro %}
