{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.quantile_transformer( ref('data_quantile_transformer') ,'col_to_transform') }}

)

select * from data
