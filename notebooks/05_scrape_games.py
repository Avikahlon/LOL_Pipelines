%pip install aiohttp selectolax beautifulsoup4 requests python-dotenv

import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")

from scraper import get_game
from utils.pipeline_monitor import log_pipeline_run
import asyncio

try:
    # read game urls from raw matches delta table
    matches_df = spark.table("lol_raw.matches")
    urls_df = matches_df.select("game_urls").collect()

    game_urls = []
    for row in urls_df:
        if row["game_urls"]:
            game_urls.extend(row["game_urls"])

    # filter already scraped
    if spark.catalog.tableExists("lol_raw.games"):
        scraped = spark.table("lol_raw.games").select("game_url").distinct().collect()
        scraped_urls = set(row["game_url"] for row in scraped)
        game_urls = [url for url in game_urls if url not in scraped_urls]

    print(f"Scraping {len(game_urls)} games...")

    chunk_size = 200
    total_chunks = (len(game_urls) + chunk_size - 1) // chunk_size

    for i in range(0, len(game_urls), chunk_size):
        chunk = game_urls[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        print(f"Processing chunk {chunk_num}/{total_chunks}")

        game_data, player_data = asyncio.run(get_game(chunk, BATCH_SIZE=200, MAX_CONCURRENT=5))

        if game_data:
            spark.createDataFrame(game_data) \
                .write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("lol_raw.games")

        if player_data:
            spark.createDataFrame(player_data) \
                .write.format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .saveAsTable("lol_raw.player_games")

    log_pipeline_run(spark, "scrape_games", "SUCCESS", len(game_urls))

except Exception as e:
    log_pipeline_run(spark, "scrape_games", "FAILED", error=e)
    raise