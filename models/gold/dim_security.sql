{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
with s1 as (
    select
        symbol,
        issue_type issue,
        s.status,
        s.name,
        exchange_id,
        sk_company_id,
        shares_outstanding,
        first_trade_date,
        first_exchange_date,
        dividend,
        s.effective_timestamp,
        s.end_timestamp,
        s.IS_CURRENT
    from
        {{ source('silver', 'securities') }} s
    join
        {{ ref("dim_company") }} c
    on 
        s.company_id = c.company_id
    and
        s.effective_timestamp between c.effective_timestamp and c.end_timestamp
)
select
     md5(cast(concat(coalesce(cast(symbol as STRING), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(effective_timestamp as STRING), '_dbt_utils_surrogate_key_null_')) as STRING)) sk_security_id,
    *
from
    s1
