{{ config(materialized='table') }}

-- unfortunately to do advanced table comparison, we have to materialize it, as bigquery cannot handle CTEs well
select 
    a.id_col,
    round(a.col_to_transform_transformed,6) as actual,
    round(b.col_to_transform_transformed,6) as expected
from {{ ref('test_quantile_transformer') }} a
join {{ ref('data_quantile_transformer_expected') }} b on a.id_col=b.id_col
