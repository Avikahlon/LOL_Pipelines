import sys

sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_tournaments
from utils.pipeline_monitor import log_pipeline_run
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

try:
    print("Scraping tournaments...")
    raw = get_tournaments()

    schema_keys = ["trname", "region", "nbgames", "avgtime", "firstgame", "lastgame", "season"]

    rows = []
    for season, tournaments in raw.items():
        for t in tournaments:
            t["season"] = season

            # map known fields
            row = {k: str(t.get(k, "")) for k in schema_keys}

            # capture unknown fields into extras
            extras = {k: str(v) for k, v in t.items() if k not in schema_keys}
            row["extras"] = json.dumps(extras) if extras else ""

            rows.append(row)

    print(f"Got {len(rows)} tournament records")

    rows = [
        {k: (str(v) if v is not None else "") for k, v in row.items()}
        for row in rows
    ]

    print(f"Got {len(rows)} tournament records")
    print("Sample tournament rows:")

    # Define explicit schema
    tournament_schema = StructType([
        StructField("trname", StringType(), True),
        StructField("region", StringType(), True),
        StructField("nbgames", StringType(), True),
        StructField("avgtime", StringType(), True),
        StructField("firstgame", StringType(), True),
        StructField("lastgame", StringType(), True),
        StructField("season", StringType(), True),
        StructField("extras", StringType(), True),
    ])

    df = spark.createDataFrame(rows, schema=tournament_schema)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.tournaments")

    log_pipeline_run(spark, "scrape_tournaments", "SUCCESS", len(rows))
    print(f"Saved {len(rows)} records to lol_raw.tournaments")

except Exception as e:
    log_pipeline_run(spark, "scrape_tournaments", "FAILED", error=e)
    raise
