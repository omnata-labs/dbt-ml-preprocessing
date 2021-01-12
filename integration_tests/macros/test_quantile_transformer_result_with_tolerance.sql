-- macro is only supported in Snowflake
{% macro snowflake__test_quantile_transformer_result_with_tolerance() %}
with a as (

    select * from {{ ref('test_quantile_transformer') }}

),

b as (

    select * from {{ ref('data_quantile_transformer_expected') }}
),

joined as(
    select round(a.col_to_transform_transformed,6) as actual,
        round(b.col_to_transform_transformed,6) as expected,
        abs(actual-expected) as difference,
        iff(difference>0,difference/b.col_to_transform_transformed,0)*100 as pc_difference
  from a
  join b on a.id_col=b.id_col
  )
select * from joined
-- The reason we tolerate tiny differences here is because of the floating point arithmetic, 
-- the values do not end up exactly the same as those output from python
where pc_difference  > 0.005
{% endmacro %}

-- other adapters we generate an empty test result to force a test pass
{% macro default__test_quantile_transformer_result_with_tolerance() %}
select 1 from (select 1) where 1=2 -- empty result set so that test passes
{% endmacro %}