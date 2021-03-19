{{ config(materialized='table') }} -- as a table because Redshift can't handle the equality checker query when it's a view

{{ dbt_ml_preprocessing.label_encoder( ref('data_label_encoder') ,'col_to_label_encode') }}

