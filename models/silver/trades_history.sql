{{ config(
    materialized='table',
    iceberg_expire_snapshots='False',
    incremental_strategy="append",
    file_format='iceberg'
) }}
select
    t_id trade_id,
    t_dts trade_timestamp,
    t_ca_id account_id,
    ts.st_name trade_status,
    tt_name trade_type,
    case t_is_cash
        when true then 'Cash'
        when false then 'Margin'
    end transaction_type,
    t_s_symb symbol,
    t_exec_name executor_name,
    t_qty quantity,
    t_bid_price bid_price,
    t_trade_price trade_price,
    t_chrg fee,
    t_comm commission,
    t_tax tax,
    us.st_name update_status,
    th_dts effective_timestamp,
    ifnull(
        lag(th_dts) over (
            partition by t_id
            order by
            th_dts desc
        ) - INTERVAL 1 milliseconds,
        to_timestamp('9999-12-31 23:59:59.999')
    ) as end_timestamp,
    CASE
        WHEN (
            row_number() over (
                partition by t_id
                order by
                th_dts desc
            ) = 1
        ) THEN TRUE
        ELSE FALSE
    END as IS_CURRENT
from
    {{source('bronze','brokerage_trade') }}
join
    {{ source('bronze','brokerage_trade_history') }}
on
    t_id = th_t_id
join
    {{ source('bronze','reference_trade_type') }}
on
    t_tt_id = tt_id
join
    {{ source('bronze','reference_status_type') }} ts
on
    t_st_id = ts.st_id
join
    {{ source('bronze','reference_status_type') }} us
on th_st_id = us.st_id