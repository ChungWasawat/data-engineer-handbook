CREATE TYPE films AS (
                     film text,
                     votes integer,
                     rating REAL,
                     filmid text
                     );
                    
CREATE TYPE quality_class AS
     ENUM ('star', 'good', 'average', 'bad');

 CREATE TABLE actors (
     actor text[],
     actorid TEXT,
     films films[],
     quality_class quality_class,
     is_active BOOLEAN,
     last_seen_year int,
     current_year int,
     PRIMARY KEY (actorid, current_year)
 );