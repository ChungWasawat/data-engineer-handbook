-- faster than using group by 4 times
SELECT *
FROM device_hits_dashboard
WHERE aggregation_level = 'os_type__device_type__browser_type'


-- CUBE
WITH events_augmented AS (
    SELECT COALESCE(d.os_type, 'unknown')      AS os_type,
           COALESCE(d.device_type, 'unknown')  AS device_type,
           COALESCE(d.browser_type, 'unknown') AS browser_type,
           url,
           user_id
    FROM events e
    JOIN devices d on e.device_id = d.device_id
)
SELECT 
        CASE 
            WHEN GROUPING(os_type)=0 AND GROUPING(device_type)=0 AND GROUPING(browser_type)=0 
                THEN 'os_type__device_type__browser_type'
            WHEN GROUPING(os_type)=0
                THEN 'os_type'
            WHEN GROUPING(device_type)=0
                THEN 'device_type'
            WHEN GROUPING(browser_type)=0 
                THEN 'browser_type'
        END AS aggregation_level,
        COALESCE(os_type, '(overall)') AS os_type,
        COALESCE(device_type, '(overall)') AS device_type,
        COALESCE(browser_type, '(overall)') AS browser_type,
        COUNT(1) as number_of_hits
FROM events_augmented
GROUP BY CUBE(os_type, device_type, browser_type)
ORDER BY COUNT(1) DESC


-- ROLLUP
WITH events_augmented AS (
    SELECT COALESCE(d.os_type, 'unknown')      AS os_type,
           COALESCE(d.device_type, 'unknown')  AS device_type,
           COALESCE(d.browser_type, 'unknown') AS browser_type,
           url,
           user_id
    FROM events e
    JOIN devices d on e.device_id = d.device_id
)
SELECT 
        CASE 
            WHEN GROUPING(os_type)=0 AND GROUPING(device_type)=0 AND GROUPING(browser_type)=0 
                THEN 'os_type__device_type__browser_type'
            WHEN GROUPING(os_type)=0
                THEN 'os_type'
            WHEN GROUPING(device_type)=0
                THEN 'device_type'
            WHEN GROUPING(browser_type)=0 
                THEN 'browser_type'
        END AS aggregation_level,
        COALESCE(os_type, '(overall)') AS os_type,
        COALESCE(device_type, '(overall)') AS device_type,
        COALESCE(browser_type, '(overall)') AS browser_type,
        COUNT(1) as number_of_hits
FROM events_augmented
GROUP BY ROLLUP(os_type, device_type, browser_type)
ORDER BY COUNT(1) DESC
