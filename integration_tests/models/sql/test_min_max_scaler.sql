{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.min_max_scaler('data_min_max_scaler','col_to_scale') }}

)

select * from data
