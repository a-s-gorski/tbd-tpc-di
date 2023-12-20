{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
select
   md5(cast(concat(coalesce(cast(trade_id as STRING), '_dbt_utils_surrogate_key_null_'), '-', coalesce(cast(t.effective_timestamp as STRING), '_dbt_utils_surrogate_key_null_')) as STRING)) sk_trade_id,
    trade_id,
    trade_status status,
    transaction_type,
    trade_type type,
    executor_name executed_by,
    t.effective_timestamp,
    t.end_timestamp,
    t.IS_CURRENT
from
    {{ source('silver', 'trades_history') }} t
    