{{ config(materialized='table') }}

-- test model is generated by adapter-specific macro, 
-- because the quantile_transformer is not supported by all DBs
{{ adapter.dispatch('quantile_transformer_model_macro')() }}
