-- min year 1970
insert into actors
with last_year_data as (
	select * from actors 
	where current_year = 2015
), this_year_data AS (
    SELECT 
    	array_agg(distinct actor) as actor,
    	actorid,
    	array_agg(row(film, votes, rating, filmid)::films) as films,
     	(CASE WHEN avg(rating) > 8 THEN 'star'
        	WHEN avg(rating) > 7 THEN 'good'
        	WHEN avg(rating) > 6 THEN 'average'
        ELSE 'bad' END)::quality_class as quality_class,
        max(year) as last_seen_year,
        max(year) is not null as is_active 
    FROM actor_films 
    WHERE year = 2016
    group by actorid
)
select 	case when ty.actor @> ly.actor then ly.actor else ly.actor || ty.actor  end as actor,
		coalesce(ly.actorid, ty.actorid) as actorid,
		case when ly.films is not null and ty.films is not null then ly.films || ty.films 
			when ly.films is not null then ly.films
			when ty.films is not null then ty.films
			else ARRAY[]::films[]
		end as films,
		coalesce(ty.quality_class, ly.quality_class) as quality_class,
		ty.is_active is not null as is_active,
		coalesce(ty.last_seen_year, ly.last_seen_year) as last_seen_year,
		2016 as current_year
from last_year_data ly
full outer join this_year_data ty
on ly.actorid = ty.actorid
order by ly.actorid


TRUNCATE table actors

select * from actor_films

select * from actors 
order by actorid



