select ct_ca_id
from {{ source('reference', 'status_type') }}
WHERE ST_NAME IS NULL