-- "/robots.txt" will show permission if the owner allows people to scrape data on the website like 'user-agent: *' means everyone, 'disallow: ' means no restriction to scrape data
 CREATE TABLE fct_users_cumulated (
-- at first, tried bigint because it's too long for integer but it's still longer than bigint so use text instead
     user_id TEXT,
-- the list of dates in the past where the user was active
     dates_active DATE[],
-- the current date for the user
     date DATE,
     PRIMARY KEY (user_id, date)
 );

INSERT INTO fct_users_cumulated
WITH yesterday AS (
    SELECT * 
    FROM fct_users_cumulated
    WHERE date = DATE('2023-01-09')
), today AS (
    SELECT  cast(user_id as text) as user_id,
            DATE(CAST(event_time as TIMESTAMP)) as date_active,
            COUNT(1) AS num_events 
    FROM events
    WHERE DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-10')
            AND user_id IS NOT NULL       
    GROUP BY user_id, DATE(CAST(event_time as TIMESTAMP))
)
select 
    COALESCE(t.user_id, y.user_id),
-- store recent date in low index is better
    CASE 
-- for new data coming
        WHEN y.dates_active is NULL THEN ARRAY[t.date_active]
-- for existing data that doesn't have new data
        WHEN t.date_active is Null Then y.dates_active
-- update new data to existing data
        ELSE ARRAY[t.date_active] || y.dates_active
    END as dates_active,
    COALESCE(t.date_active, y.date + Interval '1 day') as date
from today t
full outer join yesterday y on t.user_id = y.user_id;


drop table fct_users_cumulated

TRUNCATE table fct_users_cumulated

WITH yesterday AS (
    SELECT * 
    FROM users_cumulated
    WHERE date = DATE('2023-03-30')
), today AS (
    SELECT user_id,
            DATE_TRUNC('day', event_time) AS today_date,
            COUNT(1) AS num_events 
    FROM events
    WHERE DATE_TRUNC('day', event_time) = DATE('2023-03-31')
            AND user_id IS NOT NULL
    GROUP BY user_id,  DATE_TRUNC('day', event_time)
)
INSERT INTO users_cumulated
SELECT
-- use today id as a main one
    COALESCE(t.user_id, y.user_id),
-- if yesterday is null then will create empty array. next will concatenate with today 
    COALESCE(y.dates_active,ARRAY[]::DATE[]) || 
-- if today is new then create new array for today unless today is null then will create empty array  
    CASE 
        WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date]
        ELSE ARRAY[]::DATE[]
    END AS date_list,
-- create today value if today is null
    COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM yesterday y
FULL OUTER JOIN today t ON t.user_id = y.user_id;



