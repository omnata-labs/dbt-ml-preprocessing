{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.standard_scaler( ref('data_standard_scaler') ,'col_to_scale') }}

)

select id_col,col_to_scale,round(col_to_scale_scaled,10) as col_to_scale_scaled from data
