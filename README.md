# LoL Esports Analytics Pipeline

An end-to-end data engineering project that scrapes, transforms, and visualises professional League of Legends esports data across all regions and seasons.

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
    ↓
Power BI Dashboard
```

## Stack

| Layer | Tool |
|---|---|
| Scraping | Python, aiohttp, selectolax |
| Orchestration | Databricks Workflows |
| Storage | Delta Lake |
| Transformation | PySpark, dbt-databricks |
| Data Quality | Great Expectations, dbt tests |
| Visualisation | Power BI |
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
│   ├── 03_scrape_teams.py
│   ├── 04_scrape_matches.py
│   ├── 05_scrape_games.py
│   └── 06_load_to_staging.py
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

**Staging** — views on top of cleaned Delta tables

**Intermediate**
- `int_player_performances` — player game stats joined with match and tournament context

**Marts**
- `player_season_stats` — aggregated player stats per season and split
- `team_tournament_performance` — team win rates, objective control, and gold metrics per tournament
- `champion_meta_by_patch` — pick/ban rates and win rates per champion per patch

## Pipeline Monitoring

Every pipeline run is logged to `lol_monitoring.pipeline_runs` with status, record counts, and error details. Databricks Workflows sends email alerts on failure.

## Dashboard

The Power BI dashboard covers:
- Player performance rankings by season and region
- Team win rates and objective control trends
- Champion meta shifts by patch
- Head to head team comparisons

## Setup

1. Clone the repo
2. Install dependencies: `pip install -r requirements.txt`
3. Copy `.env.example` to `.env` and fill in credentials
4. Connect repo to Databricks via Repos
5. Run notebooks in order via Databricks Workflows
6. Run `dbt run` and `dbt test`

## Data Source

All data sourced from [Games of Legends](https://gol.gg) — the leading statistical database for professional LoL esports.