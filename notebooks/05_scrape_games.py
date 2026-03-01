%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_game
from utils.pipeline_monitor import log_pipeline_run
from pyspark.sql.types import StructType, StructField, StringType
import asyncio
import json

spark.sql("CREATE DATABASE IF NOT EXISTS lol_raw")

game_schema_keys = ["team", "result", "kills", "towers", "dragons", "barons", "gold",
                    "first_blood", "first_tower", "dragon_types", "bans", "picks", "game_url"]

player_schema_keys = ["game_url", "game_number", "player_name", "team_side", "champion",
                      "kills", "deaths", "assists", "cs"]

game_schema = StructType([StructField(k, StringType(), True) for k in game_schema_keys] +
                         [StructField("extras", StringType(), True)])

player_schema = StructType([StructField(k, StringType(), True) for k in player_schema_keys] +
                           [StructField("extras", StringType(), True)])

def build_rows(data, schema_keys):
    rows = []
    for r in data:
        row = {k: str(r.get(k, "")) for k in schema_keys}
        extras = {k: str(v) for k, v in r.items() if k not in schema_keys}
        row["extras"] = json.dumps(extras) if extras else ""
        rows.append(row)
    return rows

try:
    # read game urls from raw matches delta table
    matches_df = spark.table("lol_raw.matches")
    urls_df = matches_df.select("game_urls").collect()

    game_urls = []
    for row in urls_df:
        if row["game_urls"]:
            # game_urls stored as string so parse it back
            try:
                urls = json.loads(row["game_urls"])
                game_urls.extend(urls)
            except:
                pass

    # filter already scraped
    if spark.catalog.tableExists("lol_raw.games"):
        scraped = spark.table("lol_raw.games").select("game_url").distinct().collect()
        scraped_urls = set(row["game_url"] for row in scraped)
        game_urls = [url for url in game_urls if url not in scraped_urls]

    print(f"Scraping {len(game_urls)} games...")

    chunk_size = 200
    total_chunks = (len(game_urls) + chunk_size - 1) // chunk_size
    total_games = 0
    total_players = 0

    for i in range(0, len(game_urls), chunk_size):
        chunk = game_urls[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        print(f"Processing chunk {chunk_num}/{total_chunks}")

        game_data, player_data = asyncio.run(get_game(chunk, BATCH_SIZE=200, MAX_CONCURRENT=5))

        if game_data:
            game_rows = build_rows(game_data, game_schema_keys)
            spark.createDataFrame(game_rows, schema=game_schema) \
                .write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("lol_raw.games")
            total_games += len(game_rows)

        if player_data:
            player_rows = build_rows(player_data, player_schema_keys)
            spark.createDataFrame(player_rows, schema=player_schema) \
                .write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("lol_raw.player_games")
            total_players += len(player_rows)

        print(f"Chunk {chunk_num} done — {total_games} games, {total_players} players so far")

    log_pipeline_run(spark, "scrape_games", "SUCCESS", total_games)
    print(f"Done — {total_games} game records, {total_players} player records")

except Exception as e:
    log_pipeline_run(spark, "scrape_games", "FAILED", error=e)
    raise