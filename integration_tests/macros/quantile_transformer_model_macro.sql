-- macro is only supported in Snowflake
{% macro snowflake__quantile_transformer_model_macro() %}
with data as (

    {{ dbt_ml_preprocessing.quantile_transformer( ref('data_quantile_transformer') ,'col_to_transform') }}

)
select * from data
{% endmacro %}

-- other adapters we generate an empty test result to force a test pass
{% macro default__quantile_transformer_model_macro() %}
select 1 as empty_result from (select 1) where 1=2
{% endmacro %}