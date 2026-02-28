%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0,"/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_players
from utils.pipeline_monitor import log_pipeline_run

try:
    print("Scraping players...")
    raw = get_players()
    print(f"Got {len(raw)} player records")

    df = spark.createDataFrame(raw)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.players")

    log_pipeline_run(spark, "scrape_players", "SUCCESS", len(raw))
    print(f"Saved {len(raw)} records to lol_raw.players")

except Exception as e:
    log_pipeline_run(spark, "scrape_players", "FAILED", error=e)
    raise