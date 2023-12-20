{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
with s1 as (
    select
        *,
        to_date(transaction_timestamp) sk_transaction_date
    from
        {{ source('silver','cash_transactions') }}
)
select
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    transaction_timestamp,
    amount,
    description
from
    s1
join
    {{ ref('dim_account') }} a
on
    s1.account_id = a.account_id
and
    s1.transaction_timestamp between a.effective_timestamp and a.end_timestamp