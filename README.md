# dbt-ml-preprocessing

Example

with aggregates as (
  select max(abs(WS_LIST_PRICE)) as max_abs_dep
  from WEB_SALES)
select WS_LIST_PRICE,max_abs_dep,WS_LIST_PRICE / max_abs_dep AS WS_LIST_PRICE_SCALED
from aggregates,WEB_SALES


select {{ max_abs_scaler({{ ref('the_table') }},'WS_LIST_PRICE') }}
from ref('the_table')
