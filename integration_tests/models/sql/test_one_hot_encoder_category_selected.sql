{{ config(materialized='view') }}

{{ dbt_ml_preprocessing.one_hot_encoder( source_table=ref('data_one_hot_encoder'),
                                            source_column='column_to_encode',
                                            categories=['A','B'],
                                            handle_unknown='ignore') }}


