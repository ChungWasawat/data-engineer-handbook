-- cumulative-based: state transition tracking
-- for user growth accounting table, choosing the wrong date for first_active_date might miss some users who signed up earlier
WITH yesterday AS (
    SELECT * FROM users_growth_accounting
    WHERE date = DATE('2023-01-09')
), today AS (
    SELECT
    CAST(user_id AS TEXT) as user_id,
    DATE_TRUNC('day', event_time::timestamp) as today_date,
    COUNT(1)
    FROM events
    WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-01-10')
            AND user_id IS NOT NULL
    GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
)
insert into users_growth_accounting
SELECT COALESCE(t.user_id, y.user_id)                    as user_id,
    COALESCE(y.first_active_date, t.today_date)       AS first_active_date,
    COALESCE(t.today_date, y.last_active_date)        AS last_active_date,
    CASE
        WHEN y.user_id IS NULL 
            THEN 'New'
        WHEN y.last_active_date = t.today_date - Interval '1 day' 
            THEN 'Retained'
        WHEN y.last_active_date < t.today_date - Interval '1 day' 
            THEN 'Resurrected'
        WHEN t.today_date IS NULL AND y.last_active_date = y.date 
            THEN 'Churned'
        ELSE 'Stale'
        END                                           as daily_active_state,
    -- if there is existing cumulative table that stores online as bits, we can use bits map here
    CASE
        WHEN y.user_id IS NULL 
            THEN 'New'
        -- back to online after inactive more than 7 days since last active date
        WHEN y.last_active_date < t.today_date - Interval '7 day' 
            THEN 'Resurrected'
        -- in 7 days, no new data coming since last active date, more than 7 days = stale
        WHEN t.today_date IS NULL AND y.last_active_date = y.date - interval '7 day' 
            THEN 'Churned'
        -- t today_date = online today, y date = previous cumulation date
        -- latest active date + 7 days mustn't be earlier than previous cumulation date
        WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date 
            THEN 'Retained'
        ELSE 'Stale'
        END                                           as weekly_active_state,
    COALESCE(y.dates_active, ARRAY []::DATE[]) || 
        CASE
            WHEN t.user_id IS NOT NULL
                THEN ARRAY [t.today_date]
            ELSE ARRAY []::DATE[]
        END                                           AS date_list,
    -- latest date on cumulative
    COALESCE(t.today_date, y.date + Interval '1 day') as date
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id

-- 1 jan 23 -> 31 jan 23
select min(event_time), max(event_time)
from events

-- check latest data 10 jan 2023
select weekly_active_state, count(1)
from users_growth_accounting
where date = date('2023-01-10')
group by weekly_active_state

-- see overall weekly state data
select date, weekly_active_state, count(2)
from users_growth_accounting
group by date, weekly_active_state
order by date asc

-- troubleshoot
select date, count(1) 
from users_growth_accounting
where first_active_date = date('2023-01-01')
group by date
order by date

-- survival analysis
select  extract(dow from first_active_date) as dow,
        date - first_active_date as days_since_first_active, 
        sum(case 
                when daily_active_state in ('Retained', 'Resurrected', 'New') 
                    then 1
                else 0
            end) as number_active,
        count(1) as total_users,
        round((sum(case when daily_active_state in ('Retained', 'Resurrected', 'New') then 1 else 0 end)::Real / count(1) ) * 100) as survival_rate
from users_growth_accounting
group by extract(dow from first_active_date), date - first_active_date
order by 2, 1