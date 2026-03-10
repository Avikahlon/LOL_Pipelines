with games_with_id as (
    select
        *,
        regexp_extract(game_url, '/game/stats/(\\d+)/', 1) as game_id
    from {{ ref('stg_games') }}
),

matches_with_id as (
    select
        match_name,
        tournament,
        patch,
        date,
        team1,
        team2,
        regexp_extract(match_url, '/game/stats/(\\d+)/', 1) as game_id
    from {{ ref('stg_matches') }}
)

select
    g.game_url,
    m.tournament,
    m.match_name,
    m.patch,
    m.date,
    m.team1,
    m.team2,
    g.team,
    g.result,
    g.kills,
    g.towers,
    g.dragons,
    g.barons,
    g.gold,
    g.first_blood,
    g.first_tower,
    g.dragon_types,
    g.bans,
    g.picks
from games_with_id g
left join matches_with_id m
    on g.game_id = m.game_id