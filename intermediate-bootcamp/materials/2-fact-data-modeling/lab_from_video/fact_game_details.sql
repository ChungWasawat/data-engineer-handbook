create table fct_game_details (
    dim_game_date DATE,
    dim_season INTEGER,
    dim_team_id INTEGER,
    dim_player_id INTEGER,
    dim_player_name TEXT,
    dim_at_home BOOLEAN,
    dime_start_position TEXT,
    dim_did_not_play BOOLEAN,
    dim_did_not_dress BOOLEAN,
    dim_not_with_team BOOLEAN,
    m_minutes REAL,
    m_fgm INTEGER,
    m_fga INTEGER,
    m_fg3m INTEGER,
    m_fg3a INTEGER,
    m_ftm INTEGER,
    m_fta INTEGER,  
    m_oreb INTEGER,
    m_dreb INTEGER,
    m_reb INTEGER,
    m_ast INTEGER,
    m_stl INTEGER,
    m_blk INTEGER,
    m_turnovers INTEGER,
    m_pf INTEGER,
    m_pts INTEGER,
    m_plus_minus INTEGER,
    PRIMARY KEY (dim_game_date, dim_team_id, dim_player_id)
)

insert into fct_game_details
with deduped as ( 
    -- fact data can answer  when questions so will join games for date but not having time makes this hard to analyse
    -- want to know if how players perform in home and away games
    select 
        g.game_date_est,
        g.season,
        g.home_team_id,
        g.visitor_team_id,
        gd.*,  
        ROW_NUMBER() over (partition by gd.game_id, team_id, player_id order by g.game_date_est) as row_num
    from game_details gd
    join games g on gd.game_id = g.game_id
)
-- Select only the first occurrence of each duplicate
-- column a = column b to return boolean 
-- position('string' in column a) to check if there is the string in the column
-- split_part(column, 'delimiter', part_number) to split string into parts
-- coalesce(position('DNP' in comment) > 0, 0) didn't work because the 0 in argument can't be boolean for my postgres
-- (column a = column b) always returns integer not boolean for my postgres
select 
    game_date_est as dim_game_date,
    season as dim_season,
    team_id as dim_team_id,
    player_id as dim_player_id,
    player_name as dim_player_name,
    (team_id = home_team_id) as dim_at_home,
    start_position as dim_start_position,
    coalesce(position('DNP' in comment) >0, FALSE) as dim_did_not_play,
    coalesce(position('DND' in comment) >0, FALSE) as dim_did_not_dress,
    coalesce(position('NWT' in comment) >0, FALSE) as dim_not_with_team,
    cast(split_part(min, ':', 1) as REAL) +  split_part(min, ':', 2)::REAL/60 as m_minutes_played,
    fgm as m_fgm,
    fga as m_fga,
    fg3m as m_fg3m,
    fg3a as m_fg3a,
    ftm as m_ftm,
    fta as m_fta,
    oreb as m_oreb,
    dreb as m_dreb,
    reb as m_reb,
    ast as m_ast,
    stl as m_stl,
    blk as m_blk,
    "TO" as m_turnovers,
    pf as m_pf,
    pts as m_pts,
    plus_minus as m_plus_minus
from deduped
where row_num = 1


drop Table fct_game_details;


