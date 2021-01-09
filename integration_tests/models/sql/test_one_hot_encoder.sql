{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.one_hot_encoder( ref('data_one_hot_encoder') ,'column_to_encode') }}

)

select * from data
