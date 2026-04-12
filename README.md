# LoL Esports Analytics Pipeline

An end-to-end data engineering project that scrapes, transforms, and visualises professional League of Legends esports data across all regions and seasons.

![Databricks Pipeline](images/databricks_pipeline)

## Overview

This project builds a production-grade analytics pipeline covering over 60,000 competitive games across all major LoL esports regions from Season 3 to present. It demonstrates the full analytics engineering stack from raw data ingestion through to business intelligence dashboards.

## Architecture

```
gol.gg (source)
    ↓
Python async scraper
    ↓
Delta Tables - lol_raw (no transforms, schema evolution enabled)
    ↓
PySpark transformation notebooks
    ↓
Delta Tables - lol_staging (cleaned, typed)
    ↓
dbt models (staging → intermediate → marts)
```

## Stack

| Layer | Tool |
|---|---|
| Scraping | Python, aiohttp, selectolax |
| Orchestration | Databricks Workflows |
| Storage | Delta Lake |
| Transformation | PySpark, dbt-databricks |
| Data Quality | dbt tests |
| Version Control | GitHub |

## Data

| Dataset | Records | Description |
|---|---|---|
| Tournaments | ~1,100 | All tournaments from S3 to present |
| Matches | ~38,000 | Match results, scores, patches |
| Games | ~62,000 | Team-level game stats, picks, bans |
| Player Games | ~620,000 | Individual player performance per game |
| Players | ~43,000 | Season/split aggregated player stats |
| Teams | ~7,000 | Season/split aggregated team stats |

## Project Structure

```
lol_stats/
├── scraper.py              # all scraping functions
├── notebooks/
│   ├── 01_scrape_tournaments.py
│   ├── 02_scrape_players.py
│   ├── 03_scrape_matches.py
│   ├── 04_scrape_teams.py
│   ├── 05_scrape_games.py
│   ├── 06_load_tournaments.py
│   ├── 07_load_players.py
│   ├── 08_load_teams.py
│   ├── 09_load_matches.py
│   ├── 10_load_games.py
│   ├── 11_load_player_games.py
│   ├── 12_scrape_champ_roles.py
│   └── 13_load_champ_roles.py

├── utils/
│   └── pipeline_monitor.py
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── tests/
└── README.md
```

## dbt Models

![dbt Lineage](images/dbt_lineage)
![dbt_structure](images/dbt_files)

### Staging
Views sitting directly on top of cleaned Delta tables — no business logic, just typing and renaming.

### Intermediate
| Model | Description |
|---|---|
| `int_player_games_enriched` | Player game stats joined with match and tournament context |
| `int_player_champion_pool` | Per player per champion stats with role mapping |
| `int_team_draft_tendencies` | Per team pick/ban rates and win rates |
| `int_current_rosters` | Derived current rosters from most recent S16 games |

### Marts
| Model | Description |
|---|---|
| `mart_player_season_stats` | Aggregated player stats per season and split |
| `mart_team_season_stats` | Team win rates, objective control, gold metrics |
| `mart_match_results` | One row per match with winner, score, patch |
| `mart_game_stats` | Team-level game stats joined with tournament context |
| `mart_champion_meta` | Pick/ban/win rates per champion per season and region |
| `mart_draft_picks` | Exploded picks — one row per pick per game |
| `mart_draft_bans` | Exploded bans — one row per ban per game |

## Pipeline Monitoring

Every pipeline run is logged to `lol_monitoring.pipeline_runs` with status, record counts, and error details. Databricks Workflows sends email alerts on failure.


## Data Source

All data sourced from [Games of Legends](https://gol.gg) — the leading statistical database for professional LoL esports.
