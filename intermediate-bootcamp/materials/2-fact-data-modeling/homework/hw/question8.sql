INSERT INTO host_activity_reduced
WITH yesterday AS (
    SELECT * 
    FROM host_activity_reduced
    WHERE date = DATE('2023-01-04')
), today AS (
    select  host,
            to_char(event_time::timestamp, 'MM-YYYY') as month,
            count(1) as num_events,
            count(distinct user_id) as visitors,
            DATE(CAST(event_time as TIMESTAMP)) as date_active
    from events
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-05') 
            and host is not NULL 
            and user_id is not NULL
    group by host, month, DATE(CAST(event_time as TIMESTAMP))
)
select 
    COALESCE(t.month, y.month) as month,
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.hit_array is NULL THEN ARRAY[t.num_events]
        WHEN t.num_events is Null Then y.hit_array
        ELSE ARRAY[t.num_events] || y.hit_array
    END as hit_array,    
    CASE 
        WHEN y.unique_visitors is NULL THEN ARRAY[t.visitors]
        WHEN t.visitors is Null Then y.unique_visitors
        ELSE ARRAY[t.visitors] || y.unique_visitors
    END as unique_visitors,
    COALESCE(t.date_active, y.date + Interval '1 day') as date
from today t
full outer join yesterday y on t.host = y.host;


select host, user_id
from events

select *
from host_activity_reduced
where date = DATE('2023-01-05') 