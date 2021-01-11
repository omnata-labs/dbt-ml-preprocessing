with differences as (

    select *,
    abs(actual-expected) as difference
    from {{ ref('test_quantile_transformer_comparison') }}
),
percentage_differences as(
    select 
        *,
        if(difference>0,difference/expected,0)*100 as pc_difference
    from differences
)
select * from differences
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where pc_difference  > 0.005