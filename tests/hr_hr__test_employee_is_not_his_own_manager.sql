select *
from {{ source('hr', 'hr') }}
WHERE employee_id = manager_id