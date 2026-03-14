from pyspark.sql import functions as F

raw_df = spark.table("lol_raw.games")

def clean_float(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (~F.col(col_name).rlike("^-?[0-9]+\\.?[0-9]*$")),
        None
    ).otherwise(F.col(col_name).cast("float"))

def clean_int(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (~F.col(col_name).rlike("^-?[0-9]+$")),
        None
    ).otherwise(F.col(col_name).cast("int"))

def strip_pct(col_name):
    cleaned = F.regexp_replace(F.col(col_name), "%", "")
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (~cleaned.rlike("^-?[0-9]+\\.?[0-9]*$")),
        None
    ).otherwise(cleaned.cast("float"))

def parse_gold(col_name):
    return F.when(
        (F.col(col_name).isNull()) | (F.col(col_name) == "") | (F.col(col_name) == "-") | (F.col(col_name) == "None"),
        None
    ).otherwise(
        F.when(
            F.col(col_name).endswith("k"),
            (F.regexp_replace(F.col(col_name), "k", "").cast("float") * 1000).cast("int")
        ).otherwise(F.col(col_name).cast("int"))
    )

staging_df = raw_df.select(
    F.col("team"),
    F.col("game_url"),
    F.col("result"),
    clean_int("kills").alias("kills"),
    clean_int("towers").alias("towers"),
    clean_int("dragons").alias("dragons"),
    clean_int("barons").alias("barons"),
    parse_gold("gold").alias("gold"),
    F.when(
        (F.col("first_blood").isNull()) | (F.col("first_blood") == "") | (F.col("first_blood") == "None"),
        None
    ).otherwise(F.col("first_blood") == "True").alias("first_blood"),
    F.when(
        (F.col("first_tower").isNull()) | (F.col("first_tower") == "") | (F.col("first_tower") == "None"),
        None
    ).otherwise(F.col("first_tower") == "True").alias("first_tower"),
    F.col("dragon_types"),
    F.col("bans"),
    F.col("picks"),
    F.col("extras")
) \
.filter(F.col("game_url").isNotNull()) \
.filter(F.col("game_url") != "") \
.filter(F.col("team").isNotNull()) \
.filter(F.col("team") != "") \
.dropDuplicates(["team", "game_url"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.games")

print(f"Loaded {staging_df.count()} records to lol_staging.games")