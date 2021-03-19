{{ config(materialized='view') }}

{{ dbt_ml_preprocessing.normalizer( ref('data_normalizer') ,['col1','col2','col3','col4']) }}
