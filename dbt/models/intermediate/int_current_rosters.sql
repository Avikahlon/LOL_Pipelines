with player_team_games as (
    select
        player_name,
        team,
        MAX(date) as last_game,
        COUNT(*) as games_played
    from {{ ref('int_player_games_enriched') }}
    where season = 'S16'
    and player_name is not null
    and team is not null
    group by player_name, team
),

-- get each player's most recent team
player_current_team as (
    select
        player_name,
        team,
        last_game,
        games_played
    from player_team_games
    qualify row_number() over (partition by player_name order by last_game desc) = 1
),

-- get top 5 players per team by games played
team_rosters as (
    select
        team,
        player_name,
        last_game,
        games_played
    from player_current_team
    qualify row_number() over (partition by team order by games_played desc) <= 5
)

select
    t.team,
    t.player_name,
    t.games_played,
    t.last_game
from team_rosters t
order by t.team, t.games_played desc