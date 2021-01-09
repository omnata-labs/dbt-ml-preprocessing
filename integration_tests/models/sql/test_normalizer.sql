{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.normalizer( ref('data_normalizer') ,['col1','col2','col3','col4']) }}

)

select * from data
