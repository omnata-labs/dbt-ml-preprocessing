{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.one_hot_encoder( source_table=ref('data_one_hot_encoder'),
                                            source_column='column_to_encode',
                                            categories=['A','B']) }}

)

select * from data
