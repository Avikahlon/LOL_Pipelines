with player_teams as (
    select distinct
        e.player_name,
        e.team,
        e.game_url,
        e.season,
        e.region,
        dp.position
    from {{ ref('int_player_games_enriched') }} e
    join {{ ref('dim_player') }} dp
        on e.player_name = dp.player_name
),

opponent_bans as (
    select
        pg.player_name,
        pg.game_url,
        pg.season,
        pg.region,
        b.champion as banned_champion,
        b.ban_order
    from player_teams pt
    join {{ ref('mart_draft_bans') }} b
        on pt.game_url = b.game_url
        and b.team != pt.team
    join {{ source('lol_staging', 'champ_roles') }} cr
        on b.champion = cr.champion_name
        and (cr.primary_role = pt.position or cr.secondary_role = pt.position)
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