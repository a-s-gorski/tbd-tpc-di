{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
with s1 as (
    select 
        *,
        try_to_number(co_name_or_cik, '9999999999') as try_cik
    from {{ source("finwire", "fin") }}
)
select
    pts,
    to_number(year, '9999') as year,
    to_number(quarter, '99') as quarter,
    to_date(quarter_start_date,'yyyymmdd') as quarter_start_date,
    to_date(posting_date,'yyyymmdd') as posting_date,
    cast(revenue as float) as revenue,
    cast(earnings as float) as earnings,
    cast(eps as float) as eps,
    cast(diluted_eps as float) as diluted_eps,
    cast(margin as float) as margin,
    cast(inventory as float) as inventory,
    cast(assets as float) as assets,
    cast(liabilities as float) as liabilities,
    to_number(sh_out, '9999999999') as sh_out,
    to_number(diluted_sh_out, '9999999999') as diluted_sh_out,
    try_cik cik,
    case when try_cik is null then co_name_or_cik else null end company_name
from s1