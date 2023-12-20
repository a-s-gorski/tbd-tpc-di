{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
select *
from {{ source('brokerage', 'trade_history') }}
