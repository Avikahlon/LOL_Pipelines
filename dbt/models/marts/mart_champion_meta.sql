with pick_stats as (
    select
        champion,
        count(*) as games_picked,
        sum(win) as wins
    from {{ ref('mart_draft_picks') }}
    group by champion
),

ban_stats as (
    select
        champion,
        count(*) as games_banned
    from {{ ref('mart_draft_bans') }}
    group by champion
),

total_games as (
    select count(distinct game_url) / 2 as total
    from {{ ref('stg_games') }}
)

select
    coalesce(p.champion, b.champion) as champion,
    coalesce(p.games_picked, 0) as games_picked,
    coalesce(b.games_banned, 0) as games_banned,
    round(coalesce(p.games_picked, 0) / t.total, 3) as pick_rate,
    round(coalesce(b.games_banned, 0) / t.total, 3) as ban_rate,
    round((coalesce(p.games_picked, 0) + coalesce(b.games_banned, 0)) / t.total, 3) as presence_rate,
    case
        when coalesce(p.games_picked, 0) = 0 then null
        else round(p.wins / p.games_picked, 3)
    end as win_rate
from pick_stats p
full outer join ban_stats b on p.champion = b.champion
cross join total_games t