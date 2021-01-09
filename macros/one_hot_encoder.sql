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

select 
{% for column in include_columns %}
{{ source_table }}.{{ column }},
{% endfor %}
{% for category in category_values %}
iff({{source_column}}='{{category}}',true,false) as {{source_column}}_{{category}}
{% if not loop.last %}, {% endif %}
{% endfor %}
from {{ source_table }}
{% endmacro %}
