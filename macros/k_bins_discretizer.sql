{% macro k_bins_discretizer(source_table,source_columns,include_columns='*',n_bins=20,encode='ordinal',strategy='uniform') %}
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
{{ adapter.dispatch('k_bins_discretizer','dbt_ml_preprocessing')(source_table,source_columns,include_columns,n_bins,encode,strategy) }}
{% endmacro %}


{% macro snowflake__k_bins_discretizer(source_table,source_columns,include_columns,n_bins,encode,strategy) %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}

{% for source_column in source_columns %}
    least(width_bucket({{ source_column }},{{ source_column }}_aggregates.min_value,{{ source_column }}_aggregates.max_value,{{ n_bins }}) - 1,{{ n_bins - 1 }}) as {{ source_column }}_binned
    {% if not loop.last %}, {% endif %}
{% endfor %}
from 
  {% for source_column in source_columns %}
      {{ source_column }}_aggregates,
  {% endfor %}
  {{ source_table }} as source_table
{% endmacro %}

{% macro bigquery__k_bins_discretizer(source_table,source_columns,include_columns,n_bins,encode,strategy) %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}

{% for source_column in source_columns %}
    least(RANGE_BUCKET({{ source_column }}, GENERATE_ARRAY({{ source_column }}_aggregates.min_value, {{ source_column }}_aggregates.max_value, ({{ source_column }}_aggregates.max_value - {{ source_column }}_aggregates.min_value)/{{ n_bins }}))-1,{{ n_bins - 1 }}) as {{ source_column }}_binned
    {% if not loop.last %}, {% endif %}
{% endfor %}
from 
  {% for source_column in source_columns %}
      {{ source_column }}_aggregates,
  {% endfor %}
  {{ source_table }} as source_table
{% endmacro %}

{% macro sqlserver__k_bins_discretizer(source_table,source_columns,include_columns,n_bins,encode,strategy) %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
case when 
      floor(
          cast({{ source_column }} - {{ source_column }}_aggregates.min_value as decimal)/ cast( {{ source_column }}_aggregates.max_value - {{ source_column }}_aggregates.min_value as decimal ) * {{ n_bins }} 
      ) < {{ n_bins - 1 }}
      then floor(
          cast({{ source_column }} - {{ source_column }}_aggregates.min_value as decimal)/ cast( {{ source_column }}_aggregates.max_value - {{ source_column }}_aggregates.min_value as decimal ) * {{ n_bins }} 
      )
      else {{ n_bins - 1 }}
      end as {{ source_column }}_binned
    {% if not loop.last %}, {% endif %}
{% endfor %}
from   
  {% for source_column in source_columns %}
      {{ source_column }}_aggregates,
  {% endfor %}
  {{ source_table }} as source_table
{% endmacro %}

{% macro default__k_bins_discretizer(source_table,source_columns,include_columns,n_bins,encode,strategy) %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
least(
      floor(
          cast({{ source_column }} - {{ source_column }}_aggregates.min_value as decimal)/ cast( {{ source_column }}_aggregates.max_value - {{ source_column }}_aggregates.min_value as decimal ) * {{ n_bins }} 
      ),
      {{ n_bins - 1 }}
  ) as {{ source_column }}_binned
    {% if not loop.last %}, {% endif %}
{% endfor %}
from   
  {% for source_column in source_columns %}
      {{ source_column }}_aggregates,
  {% endfor %}
  {{ source_table }} as source_table
{% endmacro %}