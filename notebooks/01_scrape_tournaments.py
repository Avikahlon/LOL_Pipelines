%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_tournaments
from utils.pipeline_monitor import log_pipeline_run
import json

try:
    print("Scraping tournaments...")
    raw = get_tournaments()

    # flatten seasons dict into rows
    rows = []
    for season, tournaments in raw.items():
        for t in tournaments:
            t["season"] = season
            rows.append(t)

    print(f"Got {len(rows)} tournament records")
    df = spark.createDataFrame(rows)
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