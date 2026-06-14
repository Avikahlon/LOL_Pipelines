import sys

sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

import nest_asyncio

nest_asyncio.apply()
from scraper import get_matches_async, load_tournament_names
from utils.pipeline_monitor import log_pipeline_run
from pyspark.sql.types import StructType, StructField, StringType
import asyncio
import json
from datetime import datetime

schema_keys = ["match_name", "tournament", "match_url", "team1", "team2", "winner", "loser",
               "score", "match_type", "patch", "date", "BO", "game_urls"]

match_schema = StructType([StructField(k, StringType(), True) for k in schema_keys] +
                          [StructField("extras", StringType(), True)])


def log_failed_scrapes(spark, pipeline_name, failed_items):
    if not failed_items:
        return

    rows = [{
        "pipeline_name": pipeline_name,
        "identifier": str(item),
        "timestamp": datetime.utcnow().isoformat(),
        "retried": "false"
    } for item in failed_items]

    spark.createDataFrame(rows).write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_monitoring.failed_scrapes")

    print(f"Logged {len(failed_items)} failed items to lol_monitoring.failed_scrapes")


try:
    print("Loading tournament names...")
    tournaments = load_tournament_names(spark)
    print(f"Got {len(tournaments)} tournaments")

    print("Scraping matches...")
    raw, failed_tournaments = asyncio.run(get_matches_async(tournaments, max_concurrent=30))
    print(f"Got {len(raw)} match records")

    if failed_tournaments:
        print(f"{len(failed_tournaments)} tournaments failed: {failed_tournaments}")
        log_failed_scrapes(spark, "scrape_matches", failed_tournaments)

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

    if failed_tournaments:
        log_pipeline_run(spark, "scrape_matches", "PARTIAL_FAILURE",
                         len(rows), error=f"{len(failed_tournaments)} tournaments failed")

except Exception as e:
    log_pipeline_run(spark, "scrape_matches", "FAILED", error=e)
    raise