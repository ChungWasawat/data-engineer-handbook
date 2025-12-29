-- funnel analysis
-- to see how many thing/people appear in chronologically ordered stages
-- this case is to go/stay on signup page to click submit on signup page
WITH deduped_events AS (
    SELECT
        user_id, url, event_time, date(event_time) as event_date
    FROM events
    WHERE user_id IS NOT NULL 
            --AND url IN ('/signup', '/api/v1/users') -- to see overall not just these two
    GROUP BY 1,2,3,4
), self_joined as (
    SELECT d1.user_id, 
        d1.url,     
        d2.url as destination_url, 
        d1.event_time, 
        d2.event_time
    FROM deduped_events d1
    JOIN deduped_events d2 
        ON d1.user_id = d2.user_id
        AND d1.event_date = d2.event_date
        AND d2.event_time > d1.event_time
        --AND d1.url <> d2.url -- not using this because the data is not big as big data to reduce amount to compute
    WHERE d1.url = '/signup' 
), user_level as (
    SELECT 
        user_id,
        url,
        COUNT(1) AS number_of_hits,
        MAX(CASE WHEN destination_url = '/api/v1/users' THEN 1 ELSE 0 END) AS converted --boolean
    FROM self_joined
    GROUP BY user_id, url

) 
SELECT url, 
        SUM(number_of_hits) as num_hits, 
        SUM(converted) as num_converted,
        SUM(converted)::REAL / COUNT(1) as pct_converted_to_user
FROM user_level
GROUP BY url
HAVING SUM(number_of_hits) > 500


select user_id,
    lag(url) over (partition by user_id order by event_time) as source_url,
    url as destination_url,
    lag(event_time) over (partition by user_id order by event_time) as event_time1,
    event_time as event_time2,
    date(event_time) as event_date
from events
where user_id is not null
order by user_id, event_time

