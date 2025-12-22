from pyspark.sql import SparkSession

# window function requires order by in spark
# in map(), spark treats it as a struct so have to cast year to be string like other columns

query = """

WITH teams_deduped AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id ORDER BY team_id) as row_num
    FROM teams
)
SELECT
    team_id AS identifier,
    'team' AS `type`,
    map(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', CAST(yearfounded AS STRING)
        ) AS properties
FROM teams_deduped
WHERE row_num = 1

"""


def do_team_vertex_transformation(spark, dataframe):
    """create temp view to use spark sql"""
    # to test the query
    dataframe.createOrReplaceTempView("teams")
    return spark.sql(query)


def main():
    """for code execution"""
    spark = SparkSession.builder \
        .master("local") \
        .appName("players_scd") \
        .getOrCreate()
    output_df = do_team_vertex_transformation(spark, spark.table("players"))
    output_df.write.mode("overwrite").insertInto("team_vertex")
