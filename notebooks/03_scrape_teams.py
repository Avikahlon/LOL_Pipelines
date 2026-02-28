%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_teams
from utils.pipeline_monitor import log_pipeline_run

try:
    print("Scraping teams...")
    raw = get_teams()
    print(f"Got {len(raw)} team records")

    df = spark.createDataFrame(raw)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.teams")

    log_pipeline_run(spark, "scrape_teams", "SUCCESS", len(raw))
    print(f"Saved {len(raw)} records to lol_raw.teams")

except Exception as e:
    log_pipeline_run(spark, "scrape_teams", "FAILED", error=e)
    raise