from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.teams")

def strip_pct(col_name):
    return F.regexp_replace(F.col(col_name), "%", "").cast("float")

staging_df = raw_df.select(
    F.col("name").alias("team_name"),
    F.col("region"),
    F.col("season"),
    F.col("split"),
    F.col("games").cast("int"),
    (strip_pct("winrate") / 100).alias("winrate"),
    F.col("`k:d`").cast("float").alias("kda"),
    F.col("GPM").cast("int").alias("gpm"),
    F.col("GDM").cast("int").alias("gdm"),
    F.col("gameDuration").cast("int").alias("game_duration"),
    F.col("killsPerGame").cast("float").alias("kills_per_game"),
    F.col("deathsPerGame").cast("float").alias("deaths_per_game"),
    F.col("towersKilled").cast("float").alias("towers_killed"),
    F.col("towersLost").cast("float").alias("towers_lost"),
    (strip_pct("FBpercent") / 100).alias("fb_pct"),
    (strip_pct("FTpercent") / 100).alias("ft_pct"),
    (strip_pct("FOSpercent") / 100).alias("fos_pct"),
    F.col("dragsPerGame").cast("float").alias("drags_per_game"),
    (strip_pct("dragPercent") / 100).alias("drag_pct"),
    F.col("vgPerGame").cast("float").alias("vg_per_game"),
    (strip_pct("heraldPercent") / 100).alias("herald_pct"),
    (strip_pct("atakPercent") / 100).alias("atak_pct"),
    F.col("avgDrags15").cast("float").alias("avg_drags15"),
    F.col("TDat15").cast("float").alias("td_at15"),
    F.col("GDat15").cast("float").alias("gd_at15"),
    F.col("platesPerGame").cast("float").alias("plates_per_game"),
    F.col("baronPergame").cast("float").alias("baron_per_game"),
    (strip_pct("baronPercent") / 100).alias("baron_pct"),
    F.col("cspm").cast("float"),
    F.col("dpm").cast("float"),
    F.col("wpm").cast("float"),
    F.col("visionWardsPM").cast("float").alias("vision_wards_pm"),
    F.col("wardsClearedPM").cast("float").alias("wards_cleared_pm"),
    F.col("extras")
) \
.filter(F.col("name").isNotNull()) \
.filter(F.col("name") != "") \
.dropDuplicates(["team_name", "season", "split"])

# fix game_duration from mmss format to seconds
staging_df = staging_df.withColumn(
    "game_duration",
    (F.col("game_duration") / 100).cast("int") * 60 + (F.col("game_duration") % 100)
)

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.teams")

print(f"Loaded {staging_df.count()} records to lol_staging.teams")