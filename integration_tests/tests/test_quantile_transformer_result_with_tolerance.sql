with a as (

    select * from {{ ref('test_quantile_transformer') }}

),

b as (

    select * from {{ ref('data_quantile_transformer_expected') }}

),

joined as(
    select a.*,
        b.COL_TO_TRANSFORM_TRANSFORMED,
        a.col_to_transform_transformed-b.col_to_transform_transformed as difference,
        iff(difference>0,difference/b.col_to_transform_transformed,0)*100 as pc_difference
  from a
  join b on a.id_col=b.id_col
)
select * from joined
where pc_difference > 0