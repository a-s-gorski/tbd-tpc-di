{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
with t as (
    select
        ct_ca_id account_id,
        ct_dts transaction_timestamp,
        ct_amt amount,
        ct_name description
    from
        {{  source('bronze','brokerage_cash_transaction') }}
)
select
    a.customer_id,
    t.*
from
    t
join
    {{ ref('accounts') }} a
on
    t.account_id = a.account_id
and
    t.transaction_timestamp between a.effective_timestamp and a.end_timestamp

