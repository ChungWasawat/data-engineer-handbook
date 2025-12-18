CREATE TABLE hosts_cumulated (
    host TEXT,
    host_activity_datelist DATE[],
    date DATE, 
    primary key (host, date)
)

select host
from events
group by 1