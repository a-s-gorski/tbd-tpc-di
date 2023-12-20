{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
with s1 as (
    select *
    from {{ ref('fact_cash_transactions') }}
)
select 
    sk_customer_id,
    sk_account_id,
    sk_transaction_date,
    sum(amount) amount,
    description
from s1
group by sk_customer_id, sk_account_id, sk_transaction_date, description
order by sk_transaction_date, sk_customer_id, sk_account_id