%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_players
from utils.pipeline_monitor import log_pipeline_run
from pyspark.sql.types import StructType, StructField, StringType
import json

spark.sql("CREATE DATABASE IF NOT EXISTS lol_raw")

schema_keys = ["name", "link", "country", "games", "winrate", "kda", "avg_kills", "avg_deaths",
               "avg_assists", "csm", "gpm", "kp", "dmg_pct", "gold_pct", "v_pct", "dpm", "vspm",
               "wpm", "wcpm", "vwpm", "gd15", "csd15", "xpd15", "fb_pct", "fb_victim_pct",
               "penta_kills", "solo_kills", "season", "split"]

player_schema = StructType([StructField(k, StringType(), True) for k in schema_keys] +
                           [StructField("extras", StringType(), True)])

try:
    print("Scraping players...")
    raw = get_players()
    print(f"Got {len(raw)} player records")

    rows = []
    for p in raw:
        row = {k: str(p.get(k, "")) for k in schema_keys}
        extras = {k: str(v) for k, v in p.items() if k not in schema_keys}
        row["extras"] = json.dumps(extras) if extras else ""
        rows.append(row)

    df = spark.createDataFrame(rows, schema=player_schema)
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .saveAsTable("lol_raw.players")

    log_pipeline_run(spark, "scrape_players", "SUCCESS", len(rows))
    print(f"Saved {len(rows)} records to lol_raw.players")

except Exception as e:
    log_pipeline_run(spark, "scrape_players", "FAILED", error=e)
    raise