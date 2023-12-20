{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
SELECT
    md5(cast(concat(coalesce(cast(account_id as STRING), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(a.effective_timestamp as STRING), '_dbt_utils_surrogate_key_null_')) as STRING)) sk_account_id,
    a.account_id,
    sk_broker_id,
    sk_customer_id,
    a.status,
    account_desc,
    tax_status,
    a.effective_timestamp,
    a.end_timestamp,
    a.is_current
from
    {{ source('silver','accounts') }} a
join
    {{ ref('dim_customer') }} c
on a.customer_id = c.customer_id
and a.effective_timestamp between c.effective_timestamp and c.end_timestamp
join
    {{ ref('dim_broker') }} b 
using (broker_id)