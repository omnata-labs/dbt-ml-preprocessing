with a as (

    select * from {{ ref('test_quantile_transformer') }}

),

b as (

    select * from {{ ref('data_quantile_transformer_expected') }}

),

joined as(
    select a.*,
        b.COL_TO_TRANSFORM_TRANSFORMED,
        abs(a.col_to_transform_transformed-b.col_to_transform_transformed) as difference,
        iff(difference>0,difference/b.col_to_transform_transformed,0)*100 as pc_difference
  from a
  join b on a.id_col=b.id_col
)
select * from joined
-- The reason we tolerate this difference is because under the hood, when scikit-learn
-- is calculating the quantile values, it uses nanpercentile (https://github.com/scikit-learn/scikit-learn/blob/34de1b9b2122783601b245450a1885d18558ac81/sklearn/preprocessing/_data.py#L1376)
-- which, by default, uses linear interpolation when the desired percentile falls between two data points (https://github.com/numpy/numpy/blob/master/numpy/lib/nanfunctions.py#L1127)
-- Our implementation, for simplicy, uses the 'lower' strategy here, resulting in slightly different quartile values,
-- which in turn changes what is fed into the linear interpolation function in the next step
where pc_difference  > 0.5