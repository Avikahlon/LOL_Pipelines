with game_context as (
    select distinct
        game_url,
        season,
        region
    from {{ ref('int_player_games_enriched') }}
),

pick_stats as (
    select
        p.champion,
        gc.season,
        gc.region,
        count(*) as games_picked,
        sum(p.win) as wins
    from {{ ref('mart_draft_picks') }} p
    left join game_context gc on p.game_url = gc.game_url
    group by p.champion, gc.season, gc.region
),

ban_stats as (
    select
        b.champion,
        gc.season,
        gc.region,
        count(*) as games_banned
    from {{ ref('mart_draft_bans') }} b
    left join game_context gc on b.game_url = gc.game_url
    group by b.champion, gc.season, gc.region
),

total_games as (
    select
        season,
        region,
        count(distinct game_url) / 2 as total
    from game_context
    group by season, region
)

select
    coalesce(p.champion, b.champion) as champion,
    coalesce(p.season, b.season) as season,
    coalesce(p.region, b.region) as region,
    coalesce(p.games_picked, 0) as games_picked,
    coalesce(b.games_banned, 0) as games_banned,
    round(coalesce(p.games_picked, 0) / nullif(tg.total, 0), 3) as pick_rate,
    round(coalesce(b.games_banned, 0) / nullif(tg.total, 0), 3) as ban_rate,
    round((coalesce(p.games_picked, 0) + coalesce(b.games_banned, 0)) / nullif(tg.total, 0), 3) as presence_rate,
    case
        when coalesce(p.games_picked, 0) = 0 then null
        else round(p.wins / p.games_picked, 3)
    end as win_rate
from pick_stats p
full outer join ban_stats b
    on p.champion = b.champion
    and p.season = b.season
    and p.region = b.region
left join total_games tg
    on coalesce(p.season, b.season) = tg.season
    and coalesce(p.region, b.region) = tg.region