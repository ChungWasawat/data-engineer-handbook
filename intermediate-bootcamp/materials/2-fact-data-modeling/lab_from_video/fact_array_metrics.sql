CREATE TABLE fct_array_metrics(
-- use numeric when don't know which type of number to use, avoid using text
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY (user_id, month_start, metric_name)
)

-- difference between these two is delete has Write-Ahead-Log to roll back but truncate doesn't so it's faster
TRUNCATE  fct_array_metrics

DELETE FROM fct_array_metrics

insert into fct_array_metrics
-- always need yesterday's data to analyze
with daily_aggregate as (
    select user_id, 
            date(event_time) as date,
            count(1) as num_site_hits
    from events
    where date(event_time) = date('2023-01-03') 
        and user_id is not NULL
    group by user_id, date(event_time)
), yesterday_array as (
    select * 
    from fct_array_metrics 
-- no need to change if it's still the same month
    where month_start = date('2023-01-01')
)
select 
        COALESCE(da.user_id, ya.user_id) as user_id,
        COALESCE(ya.month_start, date_trunc('month', da.date)) as month_start,
        'site_hits' as metric_name,
        CASE 
            WHEN ya.metric_array is not null 
                THEN  ya.metric_array || array[COALESCE(da.num_site_hits, 0)]
            WHEN ya.metric_array is null
-- array_fill(what to fill, array) in this case will fill 0 by the number of (current date - start of month), date_trunc return as timestamp so need to convert to date again
                THEN array_fill(0, array[coalesce(date - date(date_trunc('month', date)), 0)]) || array[COALESCE(da.num_site_hits, 0)]
        END as metric_array
from daily_aggregate da  
full OUTER join yesterday_array ya on da.user_id = ya.user_id   
-- on conflict with primary keys 
-- for big data in general, this isn't used as override is used to avoid problem with update
on CONFLICT (user_id, month_start, metric_name)
do 
    update set metric_array = EXCLUDED.metric_array;

-- count length of array and group by to see if there is anomaly
select cardinality(metric_array), count(1)
from fct_array_metrics
group by 1

select *
from fct_array_metrics
where user_id = '11946491903446900000'

select * from events
where user_id = '11946491903446900000' and date(event_time) >= date('2023-01-01') and date(event_time) <= date('2023-01-04')