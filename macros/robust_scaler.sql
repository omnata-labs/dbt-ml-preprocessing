{% macro robust_scaler(source_table,source_columns,include_columns='*',with_centering=False,quantile_range=[25,75]) %}
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
{{ adapter.dispatch('robust_scaler','dbt_ml_preprocessing')(source_table,source_columns,include_columns,with_centering,quantile_range) }}
{% endmacro %}

{% macro default__robust_scaler(source_table,source_columns,include_columns,with_centering,quantile_range) %}
with 
{% for source_column in source_columns %}
    {{ source_column }}_quartiles as(
        select
            percentile_cont({{ quantile_range[0] / 100 }}) within group (order by {{ source_column }}) as first_quartile,
            percentile_cont({{ quantile_range[1] / 100 }}) within group (order by {{ source_column }}) as third_quartile
        from {{ source_table }}
    )
{% if not loop.last %}, {% endif %}
{% endfor %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
    ({{ source_column }} / ({{ source_column }}_quartiles.third_quartile - {{ source_column }}_quartiles.first_quartile)) as {{ source_column }}_scaled
    {% if not loop.last %}, {% endif %}
{% endfor %}
from 
    {% for source_column in source_columns %}
        {{ source_column }}_quartiles,
    {% endfor %}
    {{ source_table }} as source_table

{% endmacro %}

{% macro bigquery__robust_scaler(source_table,source_columns,include_columns,with_centering,quantile_range) %}
with 
{% for source_column in source_columns %}
    {{ source_column }}_quartiles as(
        select
            percentile_cont({{ source_column }},{{ quantile_range[0] / 100 }}) OVER() as first_quartile,
            percentile_cont({{ source_column }},{{ quantile_range[1] / 100 }}) OVER() as third_quartile
        from {{ source_table }}
    )
{% if not loop.last %}, {% endif %}
{% endfor %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
    ({{ source_column }} / ({{ source_column }}_quartiles.third_quartile - {{ source_column }}_quartiles.first_quartile)) as {{ source_column }}_scaled
    {% if not loop.last %}, {% endif %}
{% endfor %}
from 
    {% for source_column in source_columns %}
        {{ source_column }}_quartiles,
    {% endfor %}
    {{ source_table }} as source_table
{% endmacro %}

{% macro redshift__robust_scaler(source_table,source_columns,include_columns,with_centering,quantile_range) %}
with 
{% for source_column in source_columns %}
    {{ source_column }}_quartiles as(
        select
            percentile_cont({{ quantile_range[0] / 100 }}) within group (order by {{ source_column }}) as first_quartile,
            percentile_cont({{ quantile_range[1] / 100 }}) within group (order by {{ source_column }}) as third_quartile
        from {{ source_table }}
    )
{% if not loop.last %}, {% endif %}
{% endfor %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
    ({{ source_column }} / ({{ source_column }}_quartiles.third_quartile - {{ source_column }}_quartiles.first_quartile)) as {{ source_column }}_scaled
    {% if not loop.last %}, {% endif %}
{% endfor %}
from 
    {% for source_column in source_columns %}
        {{ source_column }}_quartiles,
    {% endfor %}
    {{ source_table }} as source_table

{% endmacro %}

{% macro sqlserver__robust_scaler(source_table,source_columns,include_columns,with_centering,quantile_range) %}
with 
{% for source_column in source_columns %}
    {{ source_column }}_quartiles as(
        select
            percentile_cont({{ quantile_range[0] / 100 }}) within group (order by {{ source_column }}) OVER() as first_quartile,
            percentile_cont({{ quantile_range[1] / 100 }}) within group (order by {{ source_column }}) OVER() as third_quartile
        from {{ source_table }}
    )
{% if not loop.last %}, {% endif %}
{% endfor %}
select 
{% for column in include_columns %}
source_table.{{ column }},
{% endfor %}
{% for source_column in source_columns %}
    ({{ source_column }} / ({{ source_column }}_quartiles.third_quartile - {{ source_column }}_quartiles.first_quartile)) as {{ source_column }}_scaled
    {% if not loop.last %}, {% endif %}
{% endfor %}
from 
    {% for source_column in source_columns %}
        {{ source_column }}_quartiles,
    {% endfor %}
    {{ source_table }} as source_table
{% endmacro %}