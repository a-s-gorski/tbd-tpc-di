select 
    sk_trade_id, 
    count(*) cnt
from {{ ref('fact_trade') }} 
group by sk_trade_id
having cnt > 1