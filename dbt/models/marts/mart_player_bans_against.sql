with player_games as (
    select
        player_name,
        team,
        game_url,
        season,
        region
    from {{ ref('int_player_games_enriched') }}
    where player_name is not null
),

opponent_bans as (
    select
        pg.player_name,
        pg.game_url,
        pg.season,
        pg.region,
        b.champion as banned_champion,
        b.ban_order
    from player_games pg
    join {{ ref('mart_draft_bans') }} b
        on pg.game_url = b.game_url
        and b.team != pg.team
)

select
    player_name,
    banned_champion,
    season,
    region,
    count(*) as times_banned,
    round(count(*) / sum(count(*)) over (partition by player_name, season), 3) as pct_of_bans
from opponent_bans
group by player_name, banned_champion, season, region
order by player_name, times_banned desc