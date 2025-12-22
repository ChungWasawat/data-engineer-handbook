INSERT INTO user_devices_cumulated
WITH yesterday AS (
    SELECT * 
    FROM user_devices_cumulated
    WHERE date = DATE('2023-01-14')
), today AS (
    select  d.device_id, 
            browser_type, 
            DATE(CAST(event_time as TIMESTAMP)) as date_active
    from devices d
    left join events e on d.device_id = e.device_id
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-15') 
            and d.device_id is not NULL
    group by d.device_id, browser_type, DATE(CAST(event_time as TIMESTAMP))
)
select 
    COALESCE(t.device_id, y.device_id) as device_id,
    COALESCE(t.browser_type, y.browser_type) as browser_type,
    CASE 
        WHEN y.device_activity_datelist is NULL THEN ARRAY[t.date_active]
        WHEN t.date_active is Null Then y.device_activity_datelist
        ELSE ARRAY[t.date_active] || y.device_activity_datelist
    END as device_activity_datelist,
    COALESCE(t.date_active, y.date + Interval '1 day') asrecorded_date
from today t
full outer join yesterday y on t.device_id = y.device_id and t.browser_type = y.browser_type;

select *
from user_devices_cumulated
where date = DATE('2023-01-15')
order by device_id

select date
from user_devices_cumulated
group by date
order by 1
