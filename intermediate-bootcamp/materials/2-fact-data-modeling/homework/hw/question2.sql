create table user_devices_cumulated(
    device_id NUMERIC,
    browser_type TEXT,
    device_activity_datelist DATE[],
    date DATE, 
    primary key (device_id, browser_type, date)
)

delete from user_devices_cumulated

select d.device_id, browser_type, event_time
from devices d
left join events e on d.device_id = e.device_id

select MAX(CAST(event_time as TIMESTAMP)), MIN(CAST(event_time as TIMESTAMP))
from devices d
left join events e on d.device_id = e.device_id
-- 1 JAN 2023 to 31 JAN 2023