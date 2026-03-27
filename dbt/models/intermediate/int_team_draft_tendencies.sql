with game_context as (
    select distinct
        game_url,
        team,
        season
    from {{ ref('int_player_games_enriched') }}
    where season in ('S15', 'S16')
),

team_picks as (
    select
        p.team,
        p.champion,
        count(*) as times_picked,
        sum(p.win) as wins,
        round(sum(p.win) / count(*), 3) as pick_win_rate
    from {{ ref('mart_draft_picks') }} p
    join game_context gc
        on p.game_url = gc.game_url
        and p.team = gc.team
    group by p.team, p.champion
),

team_bans as (
    select
        b.team,
        b.champion,
        count(*) as times_banned
    from {{ ref('mart_draft_bans') }} b
    join game_context gc
        on b.game_url = gc.game_url
        and b.team = gc.team
    group by b.team, b.champion
),

total_games as (
    select
        team,
        count(distinct game_url) as total_games
    from game_context
    group by team
)

select
    coalesce(p.team, b.team) as team,
    coalesce(p.champion, b.champion) as champion,
    coalesce(p.times_picked, 0) as times_picked,
    coalesce(b.times_banned, 0) as times_banned,
    coalesce(p.pick_win_rate, 0) as pick_win_rate,
    tg.total_games,
    round(coalesce(p.times_picked, 0) / tg.total_games, 3) as pick_rate,
    round(coalesce(b.times_banned, 0) / tg.total_games, 3) as ban_rate
from team_picks p
full outer join team_bans b
    on p.team = b.team
    and p.champion = b.champion
join total_games tg
    on coalesce(p.team, b.team) = tg.team
order by team, times_picked desc