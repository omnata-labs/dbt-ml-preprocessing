{% macro one_hot_encoder(source_table,source_column,categories='auto',handle_unknown='ignore',include_columns='*') %}
{%- if categories=='auto' -%}
    {% set category_values_query %}
    select distinct {{ source_column }} from {{ source_table }}
    order by 1
    {% endset %}
    {% set results = run_query(category_values_query) %}
    {% if execute %}
        {# Return the first column #}
        {% set category_values = results.columns[0].values() %}
    {% else %}
        {% set results_list = [] %}
    {% endif %}
{%- endif -%}
{%- if handle_unknown!='ignore' -%}
    {% set error_message %}
The `one_hot_encoder` macro only supports an 'handle_unknown' value of 'ignore' at this time.
    {% endset %}
    {%- do exceptions.raise_compiler_error(error_message) -%}
{%- endif -%}
{{ adapter.dispatch('one_hot_encoder',packages=['dbt_ml_preprocessing'])(source_table,source_column,categories,handle_unknown,include_columns) }}
{%- endmacro %}

{% macro default__one_hot_encoder(source_table,source_column,categories,handle_unknown,include_columns) %}
select 
{% for column in include_columns %}
{{ source_table }}.{{ column }},
{% endfor %}
{% for category in category_values %}
iff({{source_column}}='{{category}}',true,false) as {{source_column}}_{{category}}
{% if not loop.last %}, {% endif %}
{% endfor %}
from {{ source_table }}
{%- endmacro %}

{% macro bigquery__one_hot_encoder(source_table,source_column,categories,handle_unknown,include_columns) %}
select 
{% for column in include_columns %}
{{ column }},
{% endfor %}
{% for category in category_values %}
iff({{source_column}}='{{category}}',true,false) as {{source_column}}_{{category}}
{% if not loop.last %}, {% endif %}
{% endfor %}
from {{ source_table }}
{%- endmacro %}
