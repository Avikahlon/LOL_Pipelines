select
    {{ dbt_utils.generate_surrogate_key(['player_name']) }} as player_key,
    player_name,
    country
from {{ ref('stg_players') }}
qualify row_number() over (partition by player_name order by games desc) = 1