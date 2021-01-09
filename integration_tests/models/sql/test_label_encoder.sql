{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.label_encoder( ref('data_label_encoder') ,'col_to_label_encode') }}

)

select * from data
