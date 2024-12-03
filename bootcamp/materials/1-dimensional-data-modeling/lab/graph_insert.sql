--dml
-- vertex/ node
-- players
insert into vertices 
WITH player_agg AS (
    SELECT 
    	player_id 			as identifier, 
    	max(player_name) 	as player_name, --max and min give the same result
    	count(1) 			as number_of_games,
		sum(pts)			as total_points,
		array_agg(distinct team_id)  as teams
    FROM game_details
    group by player_id
)
SELECT
	identifier,
	'player'::vertex_type 	AS type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
        ) as properties
FROM player_agg;

--games
insert into vertices 
select 
	game_id 			as identifier,
	'game'::vertex_type as type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', case when home_team_wins = 1 then home_team_id else visitor_team_id end
	) as properties
from games g ;

-- teams
-- but data here is duplicated
insert into vertices 
WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
    FROM teams
)
select
	team_id AS identifier,
    'team'::vertex_type AS type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
        ) as properties
FROM teams_deduped
WHERE row_num = 1;


-- edge
-- player plays in game
-- but data here is duplicated
INSERT INTO edges
WITH player_game_deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
), player_game_filtered as (
	select * from player_game_deduped
	where row_num = 1
)
SELECT
	player_id 				AS subject_identifier,
	'player'::vertex_type 	as subject_type,
	game_id 				AS object_identifier,
	'game'::vertex_type 	AS object_type,
	'plays_in'::edge_type 	AS edge_type,
	json_build_object(
	    'start_position', start_position,
	    'pts', pts,
	    'team_id', team_id,
	    'team_abbreviation', team_abbreviation
	    ) as properties
FROM player_game_filtered ;

-- player plays on team
insert into edges 
with player_team_deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, team_id) AS row_num
    FROM game_details
), player_team_filtered as (
	select * from player_team_deduped
	where row_num = 1
), player_team_aggregated as (  
	select 
		player_id,
		team_id, 
		array_agg( distinct start_position) as all_position,
		sum(pts) as total_points
	from player_team_filtered
	group by player_id, team_id
)	
select 
	player_id 				AS subject_identifier,
	'player'::vertex_type 	as subject_type,
	team_id 				AS object_identifier,
	'team'::vertex_type 	AS object_type,
	'plays_on'::edge_type 	AS edge_type,
	json_build_object(
	    'all_position', all_position,
	    'total_points', total_points
	    ) as properties
from player_team_aggregated

-- player plays against another player and player shares team with another player
INSERT INTO edges
WITH player_player_deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
), player_player_filtered as (
	select * from player_player_deduped
	where row_num = 1
), player_player_aggregated as ( 
	select 
		f1.player_id 							as subject_player_id ,
		f2.player_id 							as object_player_id,
		case when f1.team_abbreviation = f2.team_abbreviation 
		then 'shares_team'::edge_type 
		else 'plays_against'::edge_type end 	as edge_type ,
		-- choose only the name they play under the most, in case they have changed
		max(f1.player_name) 					as subject_player_name,
		max(f2.player_name)						as object_player_name,
		count(1) 								as num_games,
		sum(f1.pts)								as subject_points,
		sum(f2.pts)								as object_points
	from player_player_filtered f1
	join player_player_filtered f2 
	on f1.game_id = f2.game_id and f1.player_name != f2.player_name
	where f1.player_name > f2.player_name -- make it not double edges	
	group by 
		f1.player_id ,
		f2.player_id ,	
		case when f1.team_abbreviation = f2.team_abbreviation 
		then 'shares_team'::edge_type 
		else 'plays_against'::edge_type end 	
)
SELECT
	subject_player_id 				AS subject_identifier,
	'player'::vertex_type 	as subject_type,
	object_player_id 				AS object_identifier,
	'player'::vertex_type 	AS object_type,
	edge_type 	AS edge_type,
	json_build_object(
	    'num_games', num_games,
	    'subject_points', subject_points,
	    'object_points', object_points
	    ) as properties
FROM player_player_aggregated