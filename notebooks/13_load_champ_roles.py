from pyspark.sql import functions as F

spark.sql("CREATE DATABASE IF NOT EXISTS lol_staging")

raw_df = spark.table("lol_raw.champ_roles")

staging_df = raw_df.select(
    F.col("champion_name"),
    F.col("primary_role"),
    F.col("secondary_role"),
) \
.filter(F.col("champion_name").isNotNull()) \
.filter(F.col("primary_role").isNotNull()) \
.dropDuplicates(["champion_name"])

staging_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("lol_staging.champ_roles")

print(f"Loaded {staging_df.count()} records to lol_staging.champ_roles")