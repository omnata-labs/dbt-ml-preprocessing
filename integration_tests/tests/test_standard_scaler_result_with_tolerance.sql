with a as (

    select *,
    col_to_scale_scaled as actual,
    from {{ ref('test_standard_scaler') }}

),

b as (
    
    select *,
    col_to_scale_scaled as expected,
    from {{ ref('data_standard_scaler_expected') }}

),
-- bigquery can't handle reuse of new columns later in the query, so this becomes quite messy
joined as(
    select a.*,
        b.col_to_scale_scaled,
        a.col_to_scale_scaled-b.col_to_scale_scaled as difference,
        if((a.col_to_scale_scaled-b.col_to_scale_scaled)>0,
            (a.col_to_scale_scaled-b.col_to_scale_scaled)/b.col_to_scale_scaled,
            0
        )*100 as pc_difference
  from a
  join b on a.id_col=b.id_col
)
select * from joined
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where pc_difference > 0.00000001