select
    g.game_url,
    m.tournament,
    m.patch,
    m.date,
    g.team,
    g.result,
    g.kills,
    g.towers,
    g.dragons,
    g.barons,
    g.gold,
    g.first_blood,
    g.first_tower,
    g.dragon_types,
    g.bans,
    g.picks
from {{ ref('stg_games') }} g
left join {{ ref('stg_matches') }} m
    on g.game_url like concat('%', split_part(m.match_url, '/', -2), '%')