import sys

sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

import nest_asyncio
nest_asyncio.apply()
from scraper import get_matches_async, load_tournament_names
from utils.pipeline_monitor import log_pipeline_run
from pyspark.sql.types import StructType, StructField, StringType
import asyncio
import json

schema_keys = ["match_name", "tournament", "match_url", "team1", "team2", "winner", "loser",
               "score", "match_type", "patch", "date", "BO", "game_urls"]

match_schema = StructType([StructField(k, StringType(), True) for k in schema_keys] +
                          [StructField("extras", StringType(), True)])

try:
    print("Loading tournament names...")
    tournaments = load_tournament_names(spark)
    print(f"Got {len(tournaments)} tournaments")

    print("Scraping matches...")
    raw = asyncio.run(get_matches_async(tournaments, max_concurrent=30))
    print(f"Got {len(raw)} match records")

    rows = []
    for m in raw:
        row = {k: str(m.get(k, "")) for k in schema_keys}
        extras = {k: str(v) for k, v in m.items() if k not in schema_keys}
        row["extras"] = json.dumps(extras) if extras else ""
        rows.append(row)

    df = spark.createDataFrame(rows, schema=match_schema)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.matches")

    log_pipeline_run(spark, "scrape_matches", "SUCCESS", len(rows))
    print(f"Saved {len(rows)} records to lol_raw.matches")

except Exception as e:
    log_pipeline_run(spark, "scrape_matches", "FAILED", error=e)
    raise
