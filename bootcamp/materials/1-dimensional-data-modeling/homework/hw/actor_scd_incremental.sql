insert into actors_history_scd_2
WITH past_data_scd AS (
    SELECT * FROM actors_history_scd_2
    WHERE current_year = 2015
    AND end_date = 2015
),
historical_scd AS (
    SELECT
        actorid,
        quality_class,
        is_active,
        start_date,
        end_date
    FROM actors_history_scd_2
    WHERE current_year = 2015
    AND end_date < 2015
),
present_data AS (
    SELECT * FROM actors
    WHERE current_year = 2016
),
unchanged_records AS (
    SELECT
        p.actorid,
        p.quality_class,
        p.is_active,
        ps.start_date,
        p.current_year as end_date
    FROM present_data p
    INNER JOIN past_data_scd ps
    ON ps.actorid = p.actorid
    WHERE p.quality_class = ps.quality_class
        AND p.is_active = ps.is_active
),
changed_records AS (
    SELECT
        p.actorid,
        UNNEST(ARRAY[
            ROW(
                ps.quality_class,
                ps.is_active,
                ps.start_date,
                ps.end_date
            )::scd_type,
            ROW(
                p.quality_class,
                p.is_active,
                p.current_year,
                p.current_year
            )::scd_type
        ]) as records
    FROM present_data p
    LEFT JOIN past_data_scd ps
    ON ps.actorid = p.actorid
    WHERE (p.quality_class != ps.quality_class
    OR p.is_active != ps.is_active)
),
unnested_changed_records AS (
    SELECT actorid,
        (records::scd_type).quality_class,
        (records::scd_type).is_active,
        (records::scd_type).start_date,
        (records::scd_type).end_date
    FROM changed_records
),
new_records AS (
    SELECT
        p.actorid,
        p.quality_class,
        p.is_active,
        p.current_year as start_date,
        p.current_year as end_date
    FROM present_data p
    LEFT JOIN past_data_scd ps
    ON p.actorid = ps.actorid
    WHERE ps.actorid IS NULL
)
SELECT *, 2016 AS current_year
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
order by actorid




TRUNCATE table actors_history_scd_2

select * from actors_history_scd
order by actorid, start_date

select * from actors_history_scd_2
order by actorid, current_year, start_date

