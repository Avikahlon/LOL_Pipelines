with game_results as (
    select
        game_url,
        team,
        result
    from {{ source('lol_staging', 'games') }}
),

combined as (
    select
        e.player_name,
        e.team,
        e.champion,
        e.season,
        e.region,
        e.kills,
        e.deaths,
        e.assists,
        e.cs,
        g.result
    from {{ ref('int_player_games_enriched') }} e
    left join game_results g
        on e.game_url = g.game_url
        and e.team = g.team
    where e.player_name is not null
    and e.champion is not null
)

select
    player_name,
    team,
    champion,
    season,
    region,
    count(*) as games_played,
    sum(case when result = 'WIN' then 1 else 0 end) as wins,
    round(sum(case when result = 'WIN' then 1 else 0 end) / count(*), 3) as win_rate,
    round(avg(kills), 2) as avg_kills,
    round(avg(deaths), 2) as avg_deaths,
    round(avg(assists), 2) as avg_assists,
    round(avg(cs), 1) as avg_cs,
    round(avg(case when deaths = 0 then kills + assists else (kills + assists) / deaths end), 2) as avg_kda
from combined
group by player_name, team, champion, season, region
order by player_name, games_played desc