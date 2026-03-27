import sys

sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import scrape_champion_roles
from utils.pipeline_monitor import log_pipeline_run
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

try:
    data = scrape_champion_roles()
    schema_keys = ['champion_name', 'primary_role', 'secondary_role']

    rows = []
    for champion, role_picks in sorted(data.items()):
        sorted_roles = sorted(role_picks, key=lambda x: x[1], reverse=True)[:2]

        primary_role = sorted_roles[0][0] if len(sorted_roles) > 0 else ""
        secondary_role = sorted_roles[1][0] if len(sorted_roles) > 1 else ""

        if secondary_role and sorted_roles[1][1] < 5:
            secondary_role = ""

        row = [champion, primary_role, secondary_role]

        rows.append(row)

    print(f"Got length of {len(rows)} rows")
    
    roles_schema = StructType([
        StructField("champion_name", StringType(), True),
        StructField("primary_role", StringType(), True),
        StructField("secondary_role", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema=roles_schema)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.champ_roles")

    log_pipeline_run(spark, "scrape_load_champ_roles", "SUCCESS", len(rows))
    print(f"Saved {len(rows)} records to lol_raw.champ_roles")

except Exception as e:
    log_pipeline_run(spark, "scrape_load_champ_roles", "FAILED", error=e)
    raise
