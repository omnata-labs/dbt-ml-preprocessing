{{ config(materialized='view') }}

{{ dbt_ml_preprocessing.k_bins_discretizer( ref('data_k_bins_discretizer') ,['col_to_bin_1'],n_bins=50) }}

