{{ config(materialized='view') }}

with data as (

    {{ dbt_ml_preprocessing.k_bins_discretizer( ref('data_k_bins_discretizer') ,'col_to_bin',n_bins=50) }}

)

select * from data
order by id_col
