{% macro test_equality_with_numeric_tolerance(model) %}
{#-- Prevent querying of db in parsing mode. This works because this macro does not create any new refs. #}
{%- if not execute -%}
    {{ return('') }}
{% endif %}
{%- do dbt_utils._is_relation(model, 'test_equality') -%}

{#-
If the compare_cols arg is provided, we can run this test without querying the
information schema — this allows the model to be an ephemeral model
-#}
{%- set compare_columns = kwargs.get('compare_columns', None) -%}

{%- if not compare_columns -%}
    {%- do dbt_utils._is_ephemeral(model, 'test_equality_with_numeric_tolerance') -%}
    {%- set compare_columns = adapter.get_columns_in_relation(model) | map(attribute='quoted') -%}
{%- endif -%}

{% set compare_model = kwargs.get('compare_model', kwargs.get('arg')) %}
{% set source_join_column = kwargs.get('source_join_column', kwargs.get('arg')) %}
{% set target_join_column = kwargs.get('target_join_column', kwargs.get('arg')) %}
{% set source_numeric_column_name = kwargs.get('source_numeric_column_name', kwargs.get('arg')) %}
{% set target_numeric_column_name = kwargs.get('target_numeric_column_name', kwargs.get('arg')) %}
{% set percentage_tolerance = kwargs.get('percentage_tolerance', kwargs.get('arg')) %}

{{ return(adapter.dispatch('test_equality_with_numeric_tolerance')(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,True)) }}
{% endmacro %}

{% macro default__test_equality_with_numeric_tolerance(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,output_all_rows) %}
{% set compare_cols_csv = compare_columns | join(', ') %}
with a as (
    select * from {{ model }}
),
b as (
    select * from {{ compare_model }}
),
joined as(
    select a.*,
        b.{{ target_numeric_column_name }},
        a.{{ source_numeric_column_name }}-b.{{ target_numeric_column_name }} as difference,
        if((a.{{ source_numeric_column_name }}-b.{{ target_numeric_column_name }})>0,
            (a.{{ source_numeric_column_name }}-b.{{ target_numeric_column_name }})/b.{{ target_numeric_column_name }},
            0
        )*100 as percent_difference
  from a
  join b on a.{{ source_join_column }}=b.{{ target_join_column }}
)
select {% if output_all_rows %}
        *
       {% else %}
       count(*) 
       {% endif %}
from joined
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where percent_difference > {{ percentage_tolerance }}
{% endmacro %}

{% macro postgres__test_equality_with_numeric_tolerance(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,output_all_rows) %}
{% do return( redshift__test_equality_with_numeric_tolerance(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,output_all_rows)) %}
{% endmacro %}


{% macro snowflake__test_equality_with_numeric_tolerance(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,output_all_rows) %}
{% set compare_cols_csv = compare_columns | join(', ') %}
with a as (
    select * from {{ model }}
),
b as (
    select * from {{ compare_model }}
),
joined as(
    select round(a.{{ source_numeric_column_name }},6) as actual,
        round(b.{{ target_numeric_column_name }},6) as expected,
        abs(actual-expected) as difference,
        iff(difference>0,difference/b.{{ target_numeric_column_name }},0)*100 as percent_difference
  from a
  join b on a.{{ source_join_column }}=b.{{ target_join_column }}
  )
select {% if output_all_rows %}
        *
       {% else %}
       count(*) 
       {% endif %}
from joined
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where percent_difference > {{ percentage_tolerance }}
{% endmacro %}

{% macro redshift__test_equality_with_numeric_tolerance(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,output_all_rows) %}
{% set compare_cols_csv = compare_columns | join(', ') %}
with a as (
    select * from {{ model }}
),
b as (
    select * from {{ compare_model }}
),
joined as(
    select a.*,
        b.{{ target_numeric_column_name }},
        a.{{ source_numeric_column_name }}-b.{{ target_numeric_column_name }} as difference,
        case 
            when (a.{{ source_numeric_column_name }}-b.{{ target_numeric_column_name }})>0
            then
            (a.{{ source_numeric_column_name }}-b.{{ target_numeric_column_name }})/b.{{ target_numeric_column_name }}
            else 0
            end
        *100 as percent_difference
  from a
  join b on a.{{ source_join_column }}=b.{{ target_join_column }}
)
select {% if output_all_rows %}
        *
       {% else %}
       count(*) 
       {% endif %}
from joined
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where percent_difference > {{ percentage_tolerance }}
{% endmacro %}

{% macro sqlserver__test_equality_with_numeric_tolerance(model,compare_model,source_join_column,target_join_column,source_numeric_column_name,target_numeric_column_name,percentage_tolerance,output_all_rows) %}
{% set compare_cols_csv = compare_columns | join(', ') %}
with a as (
    select * from {{ model }}
),
b as (
    select * from {{ compare_model }}
),
joined as(
    select round(a.{{ source_numeric_column_name }},6) as actual,
        round(b.{{ target_numeric_column_name }},6) as expected,
    b.{{ target_numeric_column_name }} as actual_value
  from a
  join b on a.{{ source_join_column }}=b.{{ target_join_column }}
  ),
joined_calced as(
    select 
        abs(actual-expected) as difference,
        iif(abs(actual-expected)>0,
            abs(actual-expected)/actual_value,
            0)*100 as percent_difference
  from joined
)
select {% if output_all_rows %}
        *
       {% else %}
       count(*) 
       {% endif %}
from joined_calced
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where percent_difference > {{ percentage_tolerance }}
{% endmacro %}