from pyspark.sql import functions as F

raw_df = spark.table("lol_raw.player_games")

def clean_float(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.try_cast(F.regexp_replace(F.col(col_name), "[^0-9.-]", ""), "float"))

def clean_int(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.try_cast(F.regexp_replace(F.col(col_name), "[^0-9]", ""), "int"))

def strip_pct(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.try_cast(F.regexp_replace(F.col(col_name), "[^0-9.]", ""), "float"))

staging_df = raw_df.select(
    F.col("game_url"),
    clean_int("game_number").alias("game_number"),
    F.col("player_name"),
    F.col("team"),
    F.col("team_side"),
    F.col("champion"),
    clean_int("kills").alias("kills"),
    clean_int("deaths").alias("deaths"),
    clean_int("assists").alias("assists"),
    clean_int("cs").alias("cs"),
    F.col("extras")
) \
.filter(F.col("game_url").isNotNull()) \
.filter(F.col("game_url") != "") \
.filter(F.col("player_name").isNotNull()) \
.filter(F.col("player_name") != "") \
.filter(F.col("team").isNotNull()) \
.filter(F.col("team") != "")

staging_df = staging_df.dropDuplicates(["game_url", "game_number", "player_name", "team"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.player_games")

print(f"Loaded {staging_df.count()} records to lol_staging.player_games")