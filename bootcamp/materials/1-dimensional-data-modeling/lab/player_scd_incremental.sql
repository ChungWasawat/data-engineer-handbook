-- start with min year in table actors
WITH last_season_scd AS (
    -- for changes that happend last season
    SELECT * FROM players_scd
    WHERE current_season = 2000
    AND end_season = 2000
),
historical_scd AS (
    -- for past data that won't change anymore
    SELECT
        player_name,
        scoring_class,
        is_active,
        start_season,
        end_season
    FROM players_scd
    WHERE current_season = 2000
    AND end_season < 2000
),
this_season_data AS (
    -- for this season data
    SELECT * FROM players
    WHERE current_season = 2001
),
unchanged_records AS (
    -- unchanged records between last season and this season 
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ls.start_season,
        ts.current_season as end_season
    FROM this_season_data ts
    JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE ts.scoring_class = ls.scoring_class
    AND ts.is_active = ls.is_active
),
changed_records AS (
    -- create scd type to focus on changes between start and end date
    SELECT
        ts.player_name,
        UNNEST(ARRAY[
            ROW(
                ls.scoring_class,
                ls.is_active,
                ls.start_season,
                ls.end_season
            )::scd_type,
            ROW(
                ts.scoring_class,
                ts.is_active,
                ts.current_season,
                ts.current_season
            )::scd_type
        ]) as records
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
    ON ls.player_name = ts.player_name
    WHERE (ts.scoring_class <> ls.scoring_class
    OR ts.is_active <> ls.is_active)
),
unnested_changed_records AS (
    -- choose columns to arrange in players_scd table's format with 2 rows of change of the same player
    SELECT player_name,
        (records::scd_type).scoring_class,
        (records::scd_type).is_active,
        (records::scd_type).start_season,
        (records::scd_type).end_season
    FROM changed_records
),
new_records AS (
    -- no data in last season for new players
    SELECT
        ts.player_name,
        ts.scoring_class,
        ts.is_active,
        ts.current_season AS start_season,
        ts.current_season AS end_season
    FROM this_season_data ts
    LEFT JOIN last_season_scd ls
    ON ts.player_name = ls.player_name
    WHERE ls.player_name IS NULL
)
SELECT *, 2001 AS current_season 
FROM (
    SELECT *
    FROM historical_scd
    UNION ALL
    SELECT *
    FROM unchanged_records
    UNION ALL
    SELECT *
    FROM unnested_changed_records
    UNION ALL
    SELECT *
    FROM new_records
) a