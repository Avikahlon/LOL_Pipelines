select
    tournament_name,
    region,
    season,
    number_of_games,
    game_duration,
    first_game,
    last_game
from {{ source('lol_staging', 'tournaments') }}
where tournament_name is not null