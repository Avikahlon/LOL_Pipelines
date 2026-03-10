select
    match_name,
    tournament,
    team1,
    team2,
    winner,
    loser,
    score,
    match_type,
    patch,
    date,
    bo
from {{ ref('stg_matches') }}
where winner is not null