-- high cost from window function and aggregate func
insert into players_scd
with previous_data as (
-- check changes in scoring_class and is_active between current season and previous season
	select player_name, current_season,  scoring_class, is_active, 
		lag(scoring_class, 1 ) over (partition by player_name order by current_season) as previous_scoring_class,
		lag(is_active, 1 ) over (partition by player_name order by current_season) as previous_is_active
	from players
), indicator as ( 
-- mark happend changes 
	select *, 
		case 	
			when scoring_class <> previous_scoring_class then 1
			when is_active <> previous_is_active then 1
			else 0
		end as change_indicator
	from previous_data 
), identifier as (
-- how many changes happened
	select * ,
		sum(change_indicator) over(partition by player_name order by current_season) as streak_identifier
	from indicator
)
	
-- show how many changes happened in a specific time whether players already retired
select  player_name,  scoring_class, is_active,
	min(current_season) as start_season, max(current_season) as end_season, 2016 as current_season
from identifier
group by 1,2,3
order by 1


truncate table players_scd 

select * from players_scd ps 
		
