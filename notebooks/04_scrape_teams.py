%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_teams
from utils.pipeline_monitor import log_pipeline_run
from pyspark.sql.types import StructType, StructField, StringType
import json

spark.sql("CREATE DATABASE IF NOT EXISTS lol_raw")

schema_keys = ["name", "region", "games", "winrate", "k:d", "GPM", "GDM", "gameDuration",
               "killsPerGame", "deathsPerGame", "towersKilled", "towersLost", "FBpercent",
               "FTpercent", "FOSpercent", "dragsPerGame", "dragPercent", "vgPerGame",
               "heraldPercent", "atakPercent", "avgDrags15", "TDat15", "GDat15",
               "platesPerGame", "baronPergame", "baronPercent", "cspm", "dpm",
               "wpm", "visionWardsPM", "wardsClearedPM", "season", "split"]

team_schema = StructType([StructField(k, StringType(), True) for k in schema_keys] +
                         [StructField("extras", StringType(), True)])

try:
    print("Scraping teams...")
    raw = get_teams()
    print(f"Got {len(raw)} team records")

    rows = []
    for t in raw:
        row = {k: str(t.get(k, "")) for k in schema_keys}
        extras = {k: str(v) for k, v in t.items() if k not in schema_keys}
        row["extras"] = json.dumps(extras) if extras else ""
        rows.append(row)

    df = spark.createDataFrame(rows, schema=team_schema)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.teams")

    log_pipeline_run(spark, "scrape_teams", "SUCCESS", len(rows))
    print(f"Saved {len(rows)} records to lol_raw.teams")

except Exception as e:
    log_pipeline_run(spark, "scrape_teams", "FAILED", error=e)
    raise