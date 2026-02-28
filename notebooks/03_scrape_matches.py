%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_matches_async, load_tournament_names
from utils.pipeline_monitor import log_pipeline_run
import asyncio

try:
    print("Loading tournament names...")
    tournaments = load_tournament_names()
    print(f"Got {len(tournaments)} tournaments")

    print("Scraping matches...")
    raw = asyncio.run(get_matches_async(tournaments, max_concurrent=10))
    print(f"Got {len(raw)} match records")

    df = spark.createDataFrame(raw)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.matches")

    log_pipeline_run(spark, "scrape_matches", "SUCCESS", len(raw))
    print(f"Saved {len(raw)} records to lol_raw.matches")

except Exception as e:
    log_pipeline_run(spark, "scrape_matches", "FAILED", error=e)
    raise