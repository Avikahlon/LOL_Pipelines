from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.tournaments")

staging_df = raw_df.select(
    F.col("trname").alias("tournament_name"),
    F.col("region"),
    F.col("season"),
    F.col("nbgames").cast("int").alias("number_of_games"),
    F.col("avgtime").cast("int").alias("game_duration"),
    F.to_date(F.col("firstgame"), "yyyy-MM-dd").alias("first_game"),
    F.to_date(F.col("lastgame"), "yyyy-MM-dd").alias("last_game"),
    F.col("extras")
) \
.filter(F.col("trname").isNotNull()) \
.filter(F.col("trname") != "") \
.dropDuplicates(["tournament_name", "season"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.tournaments")

print(f"Loaded {staging_df.count()} records to lol_staging.tournaments")