with recent_games as (
    select
        e.player_name,
        e.team,
        e.team_side,
        e.champion,
        e.kills,
        e.deaths,
        e.assists,
        e.cs,
        e.date,
        g.result
    from {{ ref('int_player_games_enriched') }} e
    join {{ source('lol_staging', 'games') }} g
        on e.game_url = g.game_url
        and e.team = g.team
    where e.season in ('S15', 'S16')
    and e.player_name is not null
    and e.champion is not null
)

select
    player_name,
    team,
    team_side,
    champion,
    count(*) as games_played,
    sum(case when result = 'WIN' then 1 else 0 end) as wins,
    round(sum(case when result = 'WIN' then 1 else 0 end) / count(*), 3) as win_rate,
    round(avg(kills), 2) as avg_kills,
    round(avg(deaths), 2) as avg_deaths,
    round(avg(assists), 2) as avg_assists,
    round(avg(cs), 1) as avg_cs,
    round(avg(case when deaths = 0 then kills + assists else (kills + assists) / deaths end), 2) as avg_kda,
    max(date) as last_played
from recent_games
group by player_name, team, team_side, champion
order by player_name, games_played desc