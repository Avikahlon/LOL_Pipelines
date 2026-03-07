from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.players")

def clean_float(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(F.col(col_name))

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

staging_df = raw_df.select(
    F.col("name").alias("player_name"),
    F.col("link"),
    F.col("country"),
    F.col("season"),
    F.col("split"),
    clean_int("games").alias("games"),
    (strip_pct("winrate") / 100).alias("winrate"),
    clean_float("kda").alias("kda"),
    clean_float("avg_kills").alias("avg_kills"),
    clean_float("avg_deaths").alias("avg_deaths"),
    clean_float("avg_assists").alias("avg_assists"),
    clean_float("csm").alias("csm"),
    clean_float("gpm").alias("gpm"),
    (strip_pct("kp") / 100).alias("kp"),
    (strip_pct("dmg_pct") / 100).alias("dmg_pct"),
    (strip_pct("gold_pct") / 100).alias("gold_pct"),
    (strip_pct("v_pct") / 100).alias("vision_pct"),
    clean_float("dpm").alias("dpm"),
    clean_float("vspm").alias("vspm"),
    clean_float("wpm").alias("wpm"),
    clean_float("wcpm").alias("wcpm"),
    clean_float("vwpm").alias("vwpm"),
    clean_float("gd15").alias("gd15"),
    clean_float("csd15").alias("csd15"),
    clean_float("xpd15").alias("xpd15"),
    (strip_pct("fb_pct") / 100).alias("fb_pct"),
    (strip_pct("fb_victim_pct") / 100).alias("fb_victim_pct"),
    clean_int("penta_kills").alias("penta_kills"),
    clean_int("solo_kills").alias("solo_kills"),
    F.col("extras")
) \
.filter(F.col("name").isNotNull()) \
.filter(F.col("name") != "") \
.dropDuplicates(["player_name", "season", "split"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.players")

print(f"Loaded {staging_df.count()} records to lol_staging.players")