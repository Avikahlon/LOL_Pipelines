select
    champion_name,
    primary_role,
    secondary_role
from {{ source('lol_staging', 'champ_roles') }}
where champion_name is not null