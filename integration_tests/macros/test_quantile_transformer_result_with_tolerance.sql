{% macro snowflake__test_quantile_transformer_result_with_tolerance() %}

{{ snowflake__test_equality_with_numeric_tolerance('test_quantile_transformer',
                                                    'data_quantile_transformer_expected',
                                                    'id_col',
                                                    'id_col',
                                                    'col_to_transform_transformed',
                                                    'col_to_transform_transformed',
                                                    '0.005',
                                                    output_all_rows=True) }}
{% endmacro %}

-- testing macro only works on Snowflake
{% macro default__test_quantile_transformer_result_with_tolerance() %}
select 1 as one from (select 1) where 1=2 -- empty result set so that test passes
{% endmacro %}

-- testing macro not supported in sqlserver
{% macro sqlserver__test_quantile_transformer_result_with_tolerance() %}
select null as '1' where 1=2 -- empty result set so that test passes
{% endmacro %}

-- testing macro not supported in postgres
{% macro postgres__test_quantile_transformer_result_with_tolerance() %}
select null where 1=2 -- empty result set so that test passes
{% endmacro %}