from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.players")

def strip_pct(col_name):
    return F.regexp_replace(F.col(col_name), "%", "").cast("float")

staging_df = raw_df.select(
    F.col("name").alias("player_name"),
    F.col("link"),
    F.col("country"),
    F.col("season"),
    F.col("split"),
    F.col("games").cast("int"),
    (strip_pct("winrate") / 100).alias("winrate"),
    F.col("kda").cast("float"),
    F.col("avg_kills").cast("float"),
    F.col("avg_deaths").cast("float"),
    F.col("avg_assists").cast("float"),
    F.col("csm").cast("float"),
    F.col("gpm").cast("float"),
    (strip_pct("kp") / 100).alias("kp"),
    (strip_pct("dmg_pct") / 100).alias("dmg_pct"),
    (strip_pct("gold_pct") / 100).alias("gold_pct"),
    (strip_pct("v_pct") / 100).alias("vision_pct"),
    F.col("dpm").cast("float"),
    F.col("vspm").cast("float"),
    F.col("wpm").cast("float"),
    F.col("wcpm").cast("float"),
    F.col("vwpm").cast("float"),
    F.col("gd15").cast("float"),
    F.col("csd15").cast("float"),
    F.col("xpd15").cast("float"),
    (strip_pct("fb_pct") / 100).alias("fb_pct"),
    (strip_pct("fb_victim_pct") / 100).alias("fb_victim_pct"),
    F.col("penta_kills").cast("int"),
    F.col("solo_kills").cast("int"),
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