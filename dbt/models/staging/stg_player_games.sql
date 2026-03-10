select
    game_url,
    game_number,
    player_name,
    team_side,
    champion,
    kills,
    deaths,
    assists,
    cs
from {{ source('lol_staging', 'player_games') }}
where player_name is not null