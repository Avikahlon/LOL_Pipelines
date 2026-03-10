select
    match_name,
    tournament,
    match_url,
    team1,
    team2,
    winner,
    loser,
    score,
    match_type,
    patch,
    date,
    bo,
    game_urls
from {{ source('lol_staging', 'matches') }}
where match_name is not null