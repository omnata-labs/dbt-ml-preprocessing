{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.min_max_scaler( ref('data_max_abs_scaler') ,['col_to_scale']) }}

)

select * from data
