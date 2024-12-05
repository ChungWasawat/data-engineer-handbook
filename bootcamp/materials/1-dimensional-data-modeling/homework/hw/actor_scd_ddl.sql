create table actors_history_scd
(
	actorid text,
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	current_year integer,
	primary key( actorid, start_date)
);


CREATE TYPE scd_type AS (
                    quality_class quality_class,
                    is_active boolean,
                    start_date INTEGER,
                    end_date INTEGER
                        );


create table actors_history_scd_2
(
	actorid text,
	quality_class quality_class,
	is_active boolean,
	start_date integer,
	end_date integer,
	current_year integer,
	primary key( actorid, start_date, current_year)
);

drop table actors_history_scd_2