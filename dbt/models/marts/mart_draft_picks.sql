with cleaned as (
    select
        game_url,
        team,
        result,
        regexp_replace(regexp_replace(picks, "\\[|\\]|'", ""), " ", "") as picks_clean
    from {{ ref('stg_games') }}
    where picks is not null and picks != ''
),

exploded as (
    select
        game_url,
        team,
        result,
        trim(champion) as champion,
        row_number() over (partition by game_url, team order by monotonically_increasing_id()) as pick_order
    from cleaned
    lateral view explode(split(picks_clean, ',')) as champion
)

select
    game_url,
    team,
    result,
    champion,
    pick_order,
    case when result = 'Victory' then 1 else 0 end as win
from exploded
where champion is not null
    and champion != ''
    and champion != 'Noban'