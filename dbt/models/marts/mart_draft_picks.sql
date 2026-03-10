with cleaned as (
    select
        game_url,
        team,
        result,
        split(regexp_replace(regexp_replace(picks, "\\[|\\]|'", ""), "\\s", ""), ',') as picks_array
    from {{ ref('stg_games') }}
    where picks is not null and picks != ''
),

exploded as (
    select
        game_url,
        team,
        result,
        posexplode(picks_array) as (pick_order, champion_raw)
    from cleaned
)

select
    game_url,
    team,
    result,
    trim(champion_raw) as champion,
    pick_order + 1 as pick_order,
    case when result = 'Victory' then 1 else 0 end as win
from exploded
where trim(champion_raw) is not null
    and trim(champion_raw) != ''
    and trim(champion_raw) != 'Noban'
    and trim(champion_raw) != 'No ban'