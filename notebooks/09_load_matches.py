from pyspark.sql import functions as F
import ast

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.matches")

staging_df = raw_df.select(
    F.col("match_name"),
    F.col("tournament"),
    F.col("match_url"),
    F.col("team1"),
    F.col("team2"),
    F.col("winner"),
    F.col("loser"),
    F.col("score"),
    F.col("match_type"),
    F.col("patch"),
    F.to_date(F.col("date"), "yyyy-MM-dd").alias("date"),
    F.col("BO").cast("int").alias("bo"),
    F.col("game_urls"),
    F.col("extras")
) \
.filter(F.col("match_name").isNotNull()) \
.filter(F.col("match_name") != "") \
.dropDuplicates(["match_name", "tournament", "date"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.matches")

print(f"Loaded {staging_df.count()} records to lol_staging.matches")