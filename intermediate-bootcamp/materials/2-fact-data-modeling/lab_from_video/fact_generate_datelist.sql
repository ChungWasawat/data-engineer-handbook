with users as (
-- select the latest set of data on fact table
    select * 
    from fct_users_cumulated
    where date = date('2023-01-10')
), series as (
-- create rows of interesting time ranges
    select *
    from generate_series('2023-01-01', '2023-01-10', interval '1 day') as series_date
), place_holder_ints as (
-- create binary data for 32 bits as a representative of what day users are online in range 32 bits
-- data 1 type a @> data 2 type a, given that data 1 is the bigger one which may contains data 2
-- <@ for opposite side
    select 
        CASE 
            WHEN dates_active @> ARRAY[DATE(series_date)]
                THEN  cast(pow(2, 32 - (date - DATE(series_date))) as BIGINT)
            ELSE  0
        END as placeholder_int_value,
        *
    from users
    cross join series
)
select 
    user_id, 
    cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32)) as online_dates,
    bit_count(cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32))) > 0 
        as dim_is_monthly_active,
    bit_count(cast('11111110000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32))) > 0 
        as dim_is_weekly_active,
    bit_count(cast('10000000000000000000000000000000' as bit(32)) &
        cast(cast(sum(placeholder_int_value) as BIGINT) as BIT(32))) > 0 
        as dim_is_daily_active   
from place_holder_ints
GROUP BY 1


WITH starter AS (
    SELECT uc.dates_active @> ARRAY [DATE(d.valid_date)]   AS is_active,
           EXTRACT(
               DAY FROM DATE('2023-03-31') - d.valid_date) AS days_since,
           uc.user_id
    FROM users_cumulated uc
             CROSS JOIN
         (SELECT generate_series('2023-02-28', '2023-03-31', INTERVAL '1 day') AS valid_date) as d
    WHERE date = DATE('2023-03-31')
), bits AS (
    SELECT user_id,
        SUM(CASE
                WHEN is_active THEN POW(2, 32 - days_since)
                ELSE 0 END)::bigint::bit(32) AS datelist_int,
        DATE('2023-03-31') as date
    FROM starter
    GROUP BY user_id
)