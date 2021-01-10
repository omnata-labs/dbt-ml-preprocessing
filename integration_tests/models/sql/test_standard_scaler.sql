{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.standard_scaler( ref('data_standard_scaler') ,'col_to_scale') }}

)

select * from data
