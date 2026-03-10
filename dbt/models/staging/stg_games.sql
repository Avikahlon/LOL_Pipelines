select
    game_url,
    team,
    result,
    kills,
    towers,
    dragons,
    barons,
    gold,
    first_blood,
    first_tower,
    dragon_types,
    bans,
    picks
from {{ source('lol_staging', 'games') }}
where game_url is not null