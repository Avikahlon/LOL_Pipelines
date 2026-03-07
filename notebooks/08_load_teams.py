from pyspark.sql import functions as F

raw_df = spark.table("lol_raw.teams")

def clean_float(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.col(col_name).cast("float"))

def clean_int(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.col(col_name).cast("int"))

def strip_pct(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.regexp_replace(F.col(col_name), "%", "").cast("float"))

def parse_duration(col_name):
    return F.when(
        F.col(col_name).contains(":"),
        F.split(F.col(col_name), ":")[0].cast("int") * 60 +
        F.split(F.col(col_name), ":")[1].cast("int")
    ).when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.col(col_name).cast("int"))

staging_df = raw_df.select(
    F.col("name").alias("team_name"),
    F.col("region"),
    F.col("season"),
    F.col("split"),
    clean_int("games").alias("games"),
    (strip_pct("winrate") / 100).alias("winrate"),o
    clean_float("`k:d`").alias("kda"),
    clean_float("GPM").alias("gpm"),
    clean_float("GDM").alias("gdm"),
    parse_duration("gameDuration").alias("game_duration"),
    (strip_pct("FP%") / 100).alias("fp_pct"),
    (strip_pct("BS%") / 100).alias("bs_pct"),
    clean_float("killsPerGame").alias("kills_per_game"),
    clean_float("deathsPerGame").alias("deaths_per_game"),
    clean_float("towersKilled").alias("towers_killed"),
    clean_float("towersLost").alias("towers_lost"),
    (strip_pct("FBpercent") / 100).alias("fb_pct"),
    (strip_pct("FTpercent") / 100).alias("ft_pct"),
    (strip_pct("FOSpercent") / 100).alias("fos_pct"),
    clean_float("dragsPerGame").alias("drags_per_game"),
    (strip_pct("dragPercent") / 100).alias("drag_pct"),
    clean_float("vgPerGame").alias("vg_per_game"),
    (strip_pct("heraldPercent") / 100).alias("herald_pct"),
    (strip_pct("atakPercent") / 100).alias("atak_pct"),
    clean_float("avgDrags15").alias("avg_drags15"),
    clean_float("TDat15").alias("td_at15"),
    clean_float("GDat15").alias("gd_at15"),
    clean_float("platesPerGame").alias("plates_per_game"),
    clean_float("baronPergame").alias("baron_per_game"),
    (strip_pct("baronPercent") / 100).alias("baron_pct"),
    clean_float("cspm").alias("cspm"),
    clean_float("dpm").alias("dpm"),
    clean_float("wpm").alias("wpm"),
    clean_float("visionWardsPM").alias("vision_wards_pm"),
    clean_float("wardsClearedPM").alias("wards_cleared_pm"),
    F.col("extras")
) \
.filter(F.col("name").isNotNull()) \
.filter(F.col("name") != "") \
.dropDuplicates(["team_name", "season", "split"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.teams")

print(f"Loaded {staging_df.count()} records to lol_staging.teams")