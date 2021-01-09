{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.max_abs_scaler('data_max_abs_scaler','col_to_scale') }}

)

select * from data
