import sys
sys.path.insert(0, "/Workspace/Users/abhijeetsk247@gmail.com/LOL_Pipelines")
import nest_asyncio
nest_asyncio.apply()
from scraper import get_matches_async
from pyspark.sql import functions as F
import asyncio
import json

# get failed tournaments not yet retried
failed_df = spark.table("lol_monitoring.failed_scrapes") \
    .filter((F.col("pipeline_name") == "scrape_matches") & (F.col("retried") == "false"))

failed_tournaments = [row["identifier"] for row in failed_df.collect()]
print(f"Retrying {len(failed_tournaments)} tournaments")

if failed_tournaments:
    raw, still_failed = asyncio.run(get_matches_async(failed_tournaments, max_concurrent=10))
    print(f"Recovered {len(raw)} match records, {len(still_failed)} still failed")

    # append recovered matches to lol_raw.matches
    if raw:
        schema_keys = ["match_name", "tournament", "match_url", "team1", "team2", "winner", "loser",
                       "score", "match_type", "patch", "date", "BO", "game_urls"]
        rows = []
        for m in raw:
            row = {k: str(m.get(k, "")) for k in schema_keys}
            extras = {k: str(v) for k, v in m.items() if k not in schema_keys}
            row["extras"] = json.dumps(extras) if extras else ""
            rows.append(row)

        from pyspark.sql.types import StructType, StructField, StringType

        match_schema = StructType([StructField(k, StringType(), True) for k in schema_keys] +
                                  [StructField("extras", StringType(), True)])
        recovered_df = spark.createDataFrame(rows, schema=match_schema)
        recovered_df.write.format("delta").mode("append") \
            .option("mergeSchema", "true").saveAsTable("lol_raw.matches")

    # mark all attempted as retried (whether recovered or not)
    spark.sql(f"""
        UPDATE lol_monitoring.failed_scrapes
        SET retried = 'true'
        WHERE pipeline_name = 'scrape_matches'
        AND retried = 'false'
    """)

    print(f"Marked {len(failed_tournaments)} tournaments as retried")