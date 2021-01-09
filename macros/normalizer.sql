{% macro normalizer(source_table,source_columns, include_columns='*') %}
{%- if include_columns=='*' -%}
{%- set all_source_columns = adapter.get_columns_in_relation(source_table) | map(attribute='quoted') -%}
{% set include_columns = all_source_columns | join(', ') %}
{%- endif -%}


with magnitude_calcs as (
    select 
        SQRT(
            {% for source_column in source_columns %}
            SQUARE({{ source_column }})
            {% if not loop.last %} + {% endif %}
            {% endfor %}
        ) as magnitude_calc
        ,{{include_columns}}
    from {{ source_table }}
)
select 
{{include_columns}},
{% for source_column in source_columns %}
iff(magnitude_calc=0,0,{{ source_column }}/magnitude_calc) as {{ source_column }}_NORMALIZED
{% if not loop.last %}, {% endif %}
{% endfor %}
from magnitude_calcs
{% endmacro %}