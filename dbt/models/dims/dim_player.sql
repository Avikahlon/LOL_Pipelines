with player_champion_totals as (
    select
        p.player_name,
        p.champion,
        cr.primary_role,
        sum(p.games_played) as total_games
    from {{ ref('int_player_champ_pools') }} p
    left join {{ ref('stg_champ_roles') }} cr
        on p.champion = cr.champion_name
    where cr.primary_role is not null
    group by p.player_name, p.champion, cr.primary_role
),

player_role_totals as (
    select
        player_name,
        primary_role,
        sum(total_games) as role_total_games
    from player_champion_totals
    group by player_name, primary_role
),

player_primary_role as (
    select
        player_name,
        primary_role as position
    from player_role_totals
    qualify row_number() over (partition by player_name order by role_total_games desc) = 1
),

player_info as (
    select
        player_name,
        country
    from {{ ref('stg_players') }}
    qualify row_number() over (partition by player_name order by season desc) = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['p.player_name']) }} as player_key,
    p.player_name,
    i.country,
    r.position
from (select distinct player_name from player_champion_totals) p
left join player_primary_role r on p.player_name = r.player_name
left join player_info i on p.player_name = i.player_name
order by p.player_name