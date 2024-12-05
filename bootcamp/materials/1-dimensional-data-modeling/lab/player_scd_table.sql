-- for scd incremental
-- control types of values from unnested array?
CREATE TYPE scd_type AS (
                    scoring_class scoring_class,
                    is_active boolean,
                    start_season INTEGER,
                    end_season INTEGER
                        )

create table players_scd
(
	player_name text,
	scoring_class scoring_class,
	is_active boolean,
	start_season integer,
	end_season integer,
	current_season INTEGER,
	primary key( player_name, start_season)
);

select * from players_scd

drop table players_scd 