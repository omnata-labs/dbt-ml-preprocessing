-- macro is only supported in Snowflake
{% macro snowflake__test_quantile_transformer() %}
with data as (

    {{ dbt_ml_preprocessing.quantile_transformer( ref('data_quantile_transformer') ,'col_to_transform') }}

)
select * from data
{% endmacro %}

-- other adapters we generate an empty test result to force a test pass
{% macro default__test_quantile_transformer() %}
select 1 from (select 1) where 1=2
{% endmacro %}