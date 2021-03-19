{% macro default__standard_scaler_model_macro() %}
with data as (
    {{ dbt_ml_preprocessing.standard_scaler( ref('data_standard_scaler') ,['col_to_scale_1','col_to_scale_2']) }}
)
select id_col,
        col_to_scale_1,
        col_to_scale_2,
        round(col_to_scale_1_scaled,10) as col_to_scale_1_scaled,
        round(col_to_scale_2_scaled,10) as col_to_scale_2_scaled 
from data
{% endmacro %}

-- macro is not supported in mssql
{% macro sqlserver__standard_scaler_model_macro() %}
select null as '1' where 1=2
{% endmacro %}
