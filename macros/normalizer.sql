{% macro normalizer(source_table,source_columns, include_columns='*') %}
{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns %}
{%- endif -%}

with magnitude_calcs as (
    select 
        {% for column in include_columns %}
        source_table.{{ column }},
        {% endfor %}
        SQRT(
            {% for source_column in source_columns %}
            {{ source_column }}*{{ source_column }}
            {% if not loop.last %} + {% endif %}
            {% endfor %}
        ) as magnitude_calc
    from {{ source_table }} as source_table
)
select 
{% for source_column in source_columns %}
case magnitude_calc
    when 0 then 0
    else {{ source_column }}/magnitude_calc
    end as {{ source_column }}_normalized
{% if not loop.last %}, {% endif %}
{% endfor %}
from magnitude_calcs
{% endmacro %}