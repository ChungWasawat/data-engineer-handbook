insert into hosts_cumulated
WITH yesterday AS (
    SELECT * 
    FROM hosts_cumulated
    WHERE date = DATE('2023-01-14')
), today AS (
    select  host,
            DATE(CAST(event_time as TIMESTAMP)) as date_active
    from events
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-15') 
            and host is not NULL
    group by host, DATE(CAST(event_time as TIMESTAMP))
)
select 
    COALESCE(t.host, y.host) as host,
    CASE 
        WHEN y.host_activity_datelist is NULL THEN ARRAY[t.date_active]
        WHEN t.date_active is Null Then y.host_activity_datelist
        ELSE ARRAY[t.date_active] || y.host_activity_datelist
    END as host_activity_datelist,
    COALESCE(t.date_active, y.date + Interval '1 day') as date
from today t
full outer join yesterday y on t.host = y.host;

select *
from hosts_cumulated
where date = DATE('2023-01-15')

select date
from hosts_cumulated
group by date
order by 1