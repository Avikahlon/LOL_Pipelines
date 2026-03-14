with matches_exploded as (
    select
        match_name,
        tournament,
        patch,
        date,
        game_url
    from {{ source('lol_staging', 'matches') }}
    lateral view explode(split(regexp_replace(regexp_replace(game_urls, "\\[|\\]|'", ""), "\\s+", ""), ',')) as game_url
),

tournaments as (
    select
        tournament_name,
        season,
        region
    from {{ source('lol_staging', 'tournaments') }}
)

select
    pg.game_url,
    pg.game_number,
    pg.player_name,
    pg.team,
    pg.team_side,
    pg.champion,
    pg.kills,
    pg.deaths,
    pg.assists,
    pg.cs,
    m.tournament,
    m.patch,
    m.date,
    t.season,
    t.region
from {{ source('lol_staging', 'player_games') }} pg
left join matches_exploded m on pg.game_url = m.game_url
left join tournaments t on m.tournament = t.tournament_name