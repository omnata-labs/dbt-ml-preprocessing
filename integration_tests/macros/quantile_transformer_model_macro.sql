{% macro snowflake__quantile_transformer_model_macro() %}
with data as (
    {{ dbt_ml_preprocessing.quantile_transformer( ref('data_quantile_transformer') ,'col_to_transform') }}
)
select * from data
{% endmacro %}

{% macro postgres__quantile_transformer_model_macro() %}
with data as (
    {{ dbt_ml_preprocessing.quantile_transformer( ref('data_quantile_transformer') ,'col_to_transform') }}
)
select * from data
{% endmacro %}

-- macro not supported in other databases
{% macro default__quantile_transformer_model_macro() %}
select 1 as one from (select 1) where 1=2 -- empty result set so that test passes
{% endmacro %}

{% macro redshift__quantile_transformer_model_macro() %}
select 1 as one from (select 1) where 1=2 -- empty result set so that test passes
{% endmacro %}

-- macro not supported in sqlserver
{% macro sqlserver__quantile_transformer_model_macro() %}
select null as '1' where 1=2 -- empty result set so that test passes
{% endmacro %}