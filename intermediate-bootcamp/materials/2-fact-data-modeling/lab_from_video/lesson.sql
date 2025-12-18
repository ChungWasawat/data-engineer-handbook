--day1
select dim_player_name, 
        dim_at_home,
        count(1) as num_games,
        sum(m_pts) as total_points,
        count(case when dim_not_with_team then 1 end) as bailed_num,
        count(case when dim_not_with_team then 1 end)::REAL/ count(1) as bailed_pct
from fct_game_details
group by 1,2 
order by 6 desc

--day2
select * 
from fct_users_cumulated
where date = date('2023-01-10')

--day3
-- `with ordinality` works with unnest to fill ordinality to each element in array
-- it works with `as a(text for array's name, text for ordinality's name)`
-- `cardinality` return length of array
-- expose an array after aggregating
with agg as (
        select metric_name, 
                month_start, 
                array[sum(metric_array[1]), 
                        sum(metric_array[2]),
                        sum(metric_array[3])] as summed_array
        from fct_array_metrics
        group by metric_name, month_start
)
select 
        metric_name, 
        month_start + cast(cast(index-1 as text) || 'day' as interval),
        elem as value
from agg
cross join unnest(agg.summed_array) with ordinality as a(elem, index)

with agg as (
        select metric_name, 
                month_start, 
                unnest(array[sum(metric_array[1]), 
                        sum(metric_array[2]),
                        sum(metric_array[3])]) as summed_array
        from fct_array_metrics
        group by metric_name, month_start
)
select * 
from agg



