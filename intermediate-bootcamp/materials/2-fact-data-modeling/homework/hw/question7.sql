create table host_activity_reduced (
    month TEXT,
    host text,
    hit_array INTEGER[],
    unique_visitors NUMERIC[],
    date DATE,
    primary key (month, host, date)
)

DELETE FROM host_activity_reduced