{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
select
    md5(cast(concat(coalesce(cast(employee_id as STRING), '_dbt_utils_surrogate_key_null_')) as STRING))  sk_broker_id,
    employee_id broker_id,
    manager_id,
    first_name,
    last_name,
    middle_initial,
    job_code,
    branch,
    office,
    phone
from
    {{ source('silver','employees') }}