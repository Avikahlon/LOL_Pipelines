from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.player_games")

staging_df = raw_df.select(
    F.col("game_url"),
    F.col("game_number").cast("int"),
    F.col("player_name"),
    F.col("team_side"),
    F.col("champion"),
    F.col("kills").cast("int"),
    F.col("deaths").cast("int"),
    F.col("assists").cast("int"),
    F.col("cs").cast("int"),
    F.col("extras")
) \
.filter(F.col("game_url").isNotNull()) \
.filter(F.col("game_url") != "") \
.filter(F.col("player_name").isNotNull()) \
.filter(F.col("player_name") != "") \
.dropDuplicates(["game_url", "game_number", "player_name"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.player_games")

print(f"Loaded {staging_df.count()} records to lol_staging.player_games")