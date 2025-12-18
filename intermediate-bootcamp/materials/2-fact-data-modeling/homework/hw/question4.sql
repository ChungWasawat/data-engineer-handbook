with devices as (
    select * 
    from user_devices_cumulated
    where date = date('2023-01-15')
), series as (
    select *
    from generate_series('2023-01-01', '2023-01-15', interval '1 day') as series_date
), datelist_int as (
-- create binary data for 32 bits as a representative of what day users are online in range 32 bits
-- data 1 type a @> data 2 type a, given that data 1 is the bigger one which may contains data 2
-- <@ for opposite side
    select 
        CASE 
            WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
                THEN  cast(pow(2, 32 - (date - DATE(series_date))) as BIGINT)
            ELSE  0
        END as datelist_int_value,
        *
    from devices
    cross join series
)
select 
    device_id,
    cast(cast(sum(datelist_int_value) as BIGINT) as BIT(32)) as online_dates
from datelist_int
GROUP BY 1