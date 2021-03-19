{{ config(materialized='view') }}

{{ dbt_ml_preprocessing.max_abs_scaler( ref('data_max_abs_scaler') ,['col_to_scale'],include_columns=['id_col']) }}

