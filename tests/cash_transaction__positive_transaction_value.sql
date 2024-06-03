select ct_ca_id
from {{ source('brokerage', 'cash_transaction') }}
WHERE ct_amt < 0