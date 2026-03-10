with cleaned as (
    select
        game_url,
        team,
        result,
        regexp_replace(regexp_replace(bans, "\\[|\\]|'", ""), " ", "") as bans_clean
    from {{ ref('stg_games') }}
    where bans is not null and bans != ''
),

exploded as (
    select
        game_url,
        team,
        result,
        trim(champion) as champion,
        row_number() over (partition by game_url, team order by monotonically_increasing_id()) as ban_order
    from cleaned
    lateral view explode(split(bans_clean, ',')) as champion
)

select
    game_url,
    team,
    result,
    champion,
    ban_order
from exploded
where champion is not null
    and champion != ''
    and champion != 'Noban'