with previous_data as (
	select actorid, current_year,  quality_class, is_active, 
		lag(quality_class, 1 ) over (partition by actorid order by current_year) as previous_quality_class,
		lag(is_active, 1 ) over (partition by actorid order by current_year) as previous_is_active
	from actors
), indicator as ( 
	select *, 
		case 	
			when quality_class != previous_quality_class then 1
			when is_active != previous_is_active then 1
			else 0
		end as change_indicator
	from previous_data 
), identifier as (
	select * ,
		sum(change_indicator) over(partition by actorid order by current_year) as streak_identifier
	from indicator
)
insert into actors_history_scd
select  actorid,  quality_class, is_active,
	min(current_year) as start_date, max(current_year) as end_date, 2016 as current_year
from identifier
group by actorid,  quality_class, is_active
order by actorid




TRUNCATE table actors_history_scd

select  a.actor, ah.quality_class, ah.is_active, ah.start_date, ah.end_date, ah.current_year from actors_history_scd ah
join actors a on a.actorid = ah.actorid
where ah.actorid = 'nm0000127'
order by start_date

select actor, quality_class, is_active, last_seen_year, current_year from actors 
order by actorid, current_year

