{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.robust_scaler( ref('data_robust_scaler') ,['col_to_scale']) }}

)

select * from data
