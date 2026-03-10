with cleaned as (
    select
        game_url,
        team,
        result,
        split(regexp_replace(regexp_replace(bans, "\\[|\\]|'", ""), "\\s", ""), ',') as bans_array
    from {{ ref('stg_games') }}
    where bans is not null and bans != ''
),

exploded as (
    select
        game_url,
        team,
        result,
        trim(champion) as champion,
        posexplode(bans_array) as (ban_order, champion_raw)
    from cleaned
)

select
    game_url,
    team,
    result,
    trim(champion_raw) as champion,
    ban_order + 1 as ban_order
from exploded
where trim(champion_raw) is not null
    and trim(champion_raw) != ''
    and trim(champion_raw) != 'Noban'
    and trim(champion_raw) != 'No ban'