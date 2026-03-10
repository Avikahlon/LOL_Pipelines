select
    pg.game_url,
    pg.game_number,
    pg.player_name,
    pg.team_side,
    pg.champion,
    pg.kills,
    pg.deaths,
    pg.assists,
    pg.cs,
    case
        when pg.deaths = 0 then (pg.kills + pg.assists) / 1.0
        else round((pg.kills + pg.assists) / pg.deaths, 2)
    end as kda_calc
from {{ ref('stg_player_games') }} pg
where pg.player_name is not null