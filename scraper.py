import re
from bs4 import BeautifulSoup
import requests
from urllib.parse import quote
import json
from dotenv import load_dotenv
import time
import aiohttp
import asyncio
import logging
from selectolax.lexbor import LexborHTMLParser
from selectolax.parser import HTMLParser
import nest_asyncio
nest_asyncio.apply()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

def data_cleaner(value, dtype=str):
    if value is None:
        return None

    # Convert to string only if working with text
    if isinstance(value, str):
        value = value.strip()
        if value in ("-", ""):
            return None

        # Handle mm:ss format if expecting numeric
        if ":" in value and dtype in (int, float):
            try:
                minutes, seconds = value.split(":")
                value = f"{minutes}{seconds}"
            except ValueError:
                return None

    try:
        if dtype == int:
            return int(value)
        elif dtype == float:
            return float(value)
        else:
            return str(value).strip() if isinstance(value, str) else value
    except (ValueError, TypeError):
        return None


def parse_score(score_str):
    if not score_str:
        return 0, 0
    parts = [p.strip() for p in score_str.split("-")]
    try:
        team1 = int(parts[0]) if parts[0] else 0
        team2 = int(parts[1]) if len(parts) > 1 and parts[1] else 0
        return team1, team2
    except (ValueError, IndexError):
        return 0, 0


def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "_", name)


def format_time(seconds_str):
    try:
        seconds = int(seconds_str)
        minutes = seconds // 60
        secs = seconds % 60
        return f"{minutes}:{secs:02d}"
    except (ValueError, TypeError):
        return None


def clean_text(text):
    return re.sub(r"[^\w.\s]", "", text)


def get_season_split_role_data():
    url = "https://gol.gg/players/list/season-S15/split-Spring/tournament-ALL/"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://gol.gg/",
    }

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print("Failed to load page")
        return

    soup = BeautifulSoup(response.text, 'html.parser')
    region_tables = soup.find_all("table", class_="region_filter")

    role_soup = soup.find_all(attrs={"name": "role"})
    print(role_soup)

    if len(region_tables) < 3:
        print("Expected season, split, and role filters not found.")
        return

    # --- Extract Seasons ---
    season_table = region_tables[0]
    season_links = season_table.find_all("a")
    seasons = [re.search(r'season-(S\d+)', a['href']).group(1) for a in season_links if 'season-' in a['href']]

    # --- Extract Splits ---
    split_table = region_tables[1]
    split_links = split_table.find_all("a")
    splits = [re.search(r'split-([A-Za-z\-]+)', a['href']).group(1) for a in split_links if 'split-' in a['href']]

    # --- Extract Roles ---
    role_table = region_tables[2]
    role_links = role_table.find_all("a", attrs={"role-val": True})
    roles = [a['role-val'] for a in role_links if a['role-val'] != "ALL"]

    return {
        "seasons": seasons,
        "splits": splits,
        "roles": roles
    }


# helper func
def get_seasons():
    season_page = requests.get("https://gol.gg/tournament/list/")
    soup = BeautifulSoup(season_page.text, "html.parser")

    season_elements = soup.select("li.season-link a")
    seasons = []

    for elem in season_elements:
        onclick = elem.get("onclick", "")
        season = onclick.split('"')[1]
        seasons.append(season)

    return seasons


# helper func
def load_tournament_names(spark):
    df = spark.table("lol_raw.tournaments")
    return [row["trname"] for row in df.select("trname").distinct().collect()]


# helper func
def get_urls_from_db(spark):
    df = spark.table("lol_raw.matches")
    urls = []
    for row in df.select("game_urls").collect():
        if row["game_urls"]:
            try:
                parsed = json.loads(row["game_urls"])
                if isinstance(parsed, list):
                    urls.extend(parsed)
            except:
                pass
    return urls


# Gets all tournaments and their basic details
# TODO: add Split
def get_tournaments():
    url = "https://gol.gg/tournament/ajax.trlist.php"

    headers = {
        "accept": "application/json, text/javascript, */*; q=0.01",
        "content-type": "application/x-www-form-urlencoded; charset=UTF-8",
        "x-requested-with": "XMLHttpRequest",
        "referer": "https://gol.gg/tournament/list/"
    }

    seasons = get_seasons()
    raw_data = {}

    for season in seasons:
        response = requests.post(url, headers=headers, data={"season": season})
        if response.status_code != 200:
            print(f"Failed for {season}: {response.status_code}")
            continue
        raw_data[season] = response.json()

    return raw_data


"""PLAYERS"""


# this is stats for each split/season
# TODO: for each individual split in a season(DONE)
# TODO: role base player data(rn no way of finding player role)
def get_players():
    data = get_season_split_role_data()

    seasons, splits, roles = data["seasons"], data["splits"], data["roles"]

    all_players = []

    headers_list = ["name", "link", "country", "games", "winrate", "kda", "avg_kills", "avg_deaths", "avg_assists",
                    "csm", "gpm", "kp", "dmg_pct", "gold_pct", "v_pct", "dpm", "vspm", "wpm", "wcpm", "vwpm", "gd15",
                    "csd15", "xpd15", "fb_pct",
                    "fb_victim_pct", "penta_kills", "solo_kills", "season", "split"]

    for i, season in enumerate(seasons):
        for split in splits:

            print(f"Fetching player stats for {season},{split} ...")

            url = f"https://gol.gg/players/list/season-{season}/split-{split}/tournament-ALL/"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://gol.gg/players/list/",
                "Accept-Language": "en-US,en;q=0.9",
            }

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed for {season}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find("table", class_="table_list playerslist tablesaw trhover")

            if not table:
                print(f"No player table found for {season}")
                continue

            rows = table.find_all("tr")[1:]

            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 26:
                    continue

                name_tag = cols[0].find('a')
                name = name_tag.text.strip() or ""
                link = name_tag['href']

                country = cols[1].find("span").text.strip() if cols[1].find("span") else cols[1].get_text(strip=True)

                values = [name, link, country] + [c.get_text(strip=True) for c in cols[2:26]] + [season, split]

                player_dict = dict(zip(headers_list, values))
                all_players.append(player_dict)

    # all_players = json.dumps(all_players, indent=4)

    return all_players


"""TEAMS"""


def get_teams():
    seasons = get_seasons()
    splits = ["ALL", "Pre-Season", "Winter", "Spring", "Summer"]
    all_teams = []

    known_headers = {
        "Name": "name",
        "Season": "season",
        "Region": "region",
        "Games": "games",
        "Win rate": "winrate",
        "K:D": "kd",
        "GPM": "gpm",
        "GDM": "gdm",
        "Game duration": "gameDuration",
        "FP%": "first_pick_pct",
        "Blue%": "blue_side_pct",
        "Kills / game": "killsPerGame",
        "Deaths / game": "deathsPerGame",
        "Towers killed": "towersKilled",
        "Towers lost": "towersLost",
        "FB%": "FBpercent",
        "FT%": "FTpercent",
        "DRAPG": "dragsPerGame",
        "DRA%": "dragPercent",
        "VGPG": "vgPerGame",
        "HER%": "heraldPercent",
        "DRA@15": "avgDrags15",
        "TD@15": "TDat15",
        "GD@15": "GDat15",
        "PPG": "platesPerGame",
        "NASHPG": "baronPergame",
        "NASH%": "baronPercent",
        "CSM": "cspm",
        "DPM": "dpm",
        "WPM": "wpm",
        "VWPM": "visionWardsPM",
        "WCPM": "wardsClearedPM",
    }

    for split in splits:
        for i, season in enumerate(seasons):
            if i <= 2:
                continue

            url = f"https://gol.gg/teams/list/season-{season}/split-{split}/tournament-ALL/"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                "Referer": "https://gol.gg/players/list/",
                "Accept-Language": "en-US,en;q=0.9",
            }

            response = requests.get(url, headers=headers)

            if response.status_code != 200:
                print(f"Failed for {season}: {response.status_code}")
                continue

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find("table", class_="table_list playerslist tablesaw trhover")

            if not table:
                print(f"No team table found for {season}")
                continue

            # get headers once per page
            page_headers = [th.get_text(strip=True) for th in table.find_all("th")]

            for row in table.find_all("tr")[1:]:
                cols = row.find_all("td")
                if len(cols) < 2:
                    continue

                team = {"split": split}
                extras = {}

                for j, col in enumerate(cols):
                    if j >= len(page_headers):
                        break
                    page_header = page_headers[j]
                    value = col.get_text(strip=True)

                    if page_header in known_headers:
                        team[known_headers[page_header]] = value
                    else:
                        extras[page_header] = value

                # season from table takes priority, fall back to loop variable
                if "season" not in team:
                    team["season"] = season

                team["extras"] = json.dumps(extras) if extras else ""

                if team.get("name", ""):
                    all_teams.append(team)

    return all_teams


"""GAME DATA"""


async def fetch_game(session, url, semaphore, headers):
    async with semaphore:
        try:
            async with session.get(url, headers=headers, timeout=15) as resp:
                resp.raise_for_status()
                html = await resp.text()
                return url, html
        except Exception as e:
            print(f"Failed for {url}: {e}")
            return url, None


def split_batches(lst, batch_size):
    """Split a list into chunks."""
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]


async def process_batch(session, batch, headers, semaphore):
    tasks = [fetch_game(session, url, semaphore, headers) for url in batch]
    results = await asyncio.gather(*tasks)

    all_games_data = []
    all_players_data = []

    for index, (url, html) in enumerate(results):
        if not html:
            continue

        tree = HTMLParser(html)

        game_data = extract_team_game_data(tree, url)
        player_data = extract_player_game_data(tree, url)

        all_games_data.extend(game_data)
        all_players_data.extend(player_data)

    return all_games_data, all_players_data


async def get_game(game_urls, BATCH_SIZE=500, MAX_CONCURRENT=10):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Referer": "https://gol.gg/game/stats/",
        "Accept-Language": "en-US,en;q=0.9",
    }

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    all_games_data = []
    all_players_data = []

    start_time = time.time()

    batches = list(split_batches(game_urls, BATCH_SIZE))
    async with aiohttp.ClientSession() as session:
        for i, batch in enumerate(batches, 1):
            print(f"Processing batch {i}/{len(batches)}...")
            batch_start = time.time()
            games_data, players_data = await process_batch(session, batch, headers, semaphore)
            all_games_data.extend(games_data)
            all_players_data.extend(players_data)
            print(f"Batch {i} done in {time.time() - batch_start:.2f} seconds")
            predicted_total = (time.time() - start_time) / i * len(batches)
            print(f"Predicted total time: {predicted_total:.2f} seconds")

    print(f"Total time: {time.time() - start_time:.2f} seconds")

    return all_games_data, all_players_data


def extract_team_game_data(tree: HTMLParser, game_url: str):
    game_data = []

    teams = tree.css("div.row.rowbreak > div.col-12.col-sm-6")
    for side in teams:
        header = side.css_first("div.blue-line-header") or side.css_first("div.red-line-header")
        if not header:
            continue

        team_name = header.css_first("a").text(strip=True)
        result = "WIN" if "WIN" in header.text() else "LOSS"
        # logger.info(f"Processing {team_name} - {result}")

        stats_row = side.css_first("div.row[style*='min-height']")
        if not stats_row:
            continue

        stats = stats_row.css("span.score-box")

        kills = towers = dragons = barons = gold = None
        for span in stats:
            img = span.css_first("img")
            if not img:
                continue

            alt_text = img.attributes.get("alt", "")
            val = span.text(strip=True).split()[-1]

            if "Kills" in alt_text:
                kills = int(val) if val.isdigit() else None
            elif "Towers" in alt_text:
                towers = int(val) if val.isdigit() else None
            elif "Dragons" in alt_text:
                dragons = int(val) if val.isdigit() else None
            elif "Nashor" in alt_text:
                barons = int(val) if val.isdigit() else None
            elif "Gold" in alt_text:
                gold = val  # Keep as string - "69.5k", "62k", etc.

        # Fixed: Look for first blood/tower in the entire side, not just header
        # They appear as separate img tags with champion_icon_light class
        all_imgs = side.css("img.champion_icon_light")
        first_blood = any("Blood" in img.attributes.get("alt", "") for img in all_imgs)
        first_tower = any("Tower" in img.attributes.get("alt", "") for img in all_imgs)

        # Dragons with XS class
        dragon_imgs = stats_row.css("img.champion_icon_XS")
        dragon_types = [
            img.attributes.get("alt") or ""
            for img in dragon_imgs
            if img.attributes.get("alt")
        ]

        rows = side.css("div.row")
        bans, picks = [], []

        for row in rows:
            label = row.css_first("div.col-2")
            if not label:
                continue

            label_text = label.text(strip=True)
            if label_text == "Bans":
                ban_imgs = row.css("div.col-10 img.champion_icon_medium")
                bans = [
                    img.attributes.get("alt") or ""
                    for img in ban_imgs
                    if img.attributes.get("alt")
                ]
            elif label_text == "Picks":
                pick_imgs = row.css("div.col-10 img.champion_icon_medium")
                picks = [
                    img.attributes.get("alt") or ""
                    for img in pick_imgs
                    if img.attributes.get("alt")
                ]

        game_data.append({
            "team": team_name,
            "result": result,
            "kills": kills,
            "towers": towers,
            "dragons": dragons,
            "barons": barons,
            "gold": gold,
            "first_blood": first_blood,
            "first_tower": first_tower,
            "dragon_types": dragon_types,
            "bans": bans,
            "picks": picks,
            "game_url": game_url
        })

    return game_data


def extract_player_game_data(tree: HTMLParser, game_url: str):
    players_data = []

    # get team names from blue/red headers
    blue_header = tree.css_first("div.blue-line-header a")
    red_header = tree.css_first("div.red-line-header a")
    blue_team = blue_header.text(strip=True) if blue_header else ""
    red_team = red_header.text(strip=True) if red_header else ""

    active_button = tree.css_first("li.game-menu-button-active a")
    game_number = 1
    if active_button:
        text = active_button.text(strip=True)
        if "Game" in text:
            try:
                game_number = int(text.replace("Game", "").strip())
            except ValueError:
                game_number = 1

    tables = tree.css("table.playersInfosLine")
    if not tables or len(tables) < 2:
        return players_data

    for side, table in zip(["blue", "red"], tables):
        team_name = blue_team if side == "blue" else red_team
        rows = table.css("tr")

        for row in rows:
            cols = row.css("td")
            if len(cols) < 4:
                continue

            champ_tag = cols[0].css_first("a[href*='champion-stats']")
            player_tag = cols[0].css_first("a[href*='player-stats']")

            champion = champ_tag.attrs.get("title", "").replace(" stats", "") if champ_tag else None
            player_name = player_tag.text(strip=True) if player_tag else None
            player_url = player_tag.attrs.get("href", "") if player_tag else ""
            player_id = player_url.strip("/").split("/")[2] if player_url else None

            kda_text = cols[-2].text(strip=True)
            try:
                kills, deaths, assists = map(int, kda_text.split("/"))
            except ValueError:
                kills = deaths = assists = 0

            cs_text = cols[-1].text(strip=True)
            try:
                cs = int(cs_text.split()[0])
            except (ValueError, IndexError):
                cs = 0

            players_data.append({
                "game_url": game_url,
                "game_number": game_number,
                "player_name": player_name,
                "team": team_name,
                "team_side": side,
                "champion": champion,
                "kills": kills,
                "deaths": deaths,
                "assists": assists,
                "cs": cs
            })

    return players_data


"""MATCHES"""

def get_bo_(score):
    if not score or "-" not in score:
        return None
    try:
        parts = [int(s.strip()) for s in score.split("-")]
        return max(parts) * 2 - 1  # e.g. 2-1 → BO3, 3-2 → BO5
    except ValueError:
        return None

async def fetch(session, url, semaphore, headers=None, retries=3):
    async with semaphore:
        for attempt in range(retries):
            try:
                timeout = aiohttp.ClientTimeout(total=60)
                async with session.get(url, headers=headers, timeout=timeout) as response:
                    if response.status != 200:
                        print(f"Failed {url}: {response.status}")
                        return None
                    return await response.text()
            except asyncio.TimeoutError:
                print(f"Timeout fetching {url} (attempt {attempt + 1}/{retries})")
                if attempt < retries - 1:
                    await asyncio.sleep(2)
                continue
            except Exception as e:
                print(f"Error fetching {url}: {e}")
                return None
        print(f"All retries failed for {url}")
        return None


async def get_game_url(session, match_url, score, retries=3):
    for attempt in range(retries):
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with session.get(match_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout) as resp:
                html = await resp.text()
                tree = LexborHTMLParser(html)
                game_links = tree.css("li.game-menu-button a[href*='/page-game/']")
                urls = [
                    "https://gol.gg" + link.attributes.get("href", "").replace("..", "")
                    for link in game_links
                    if link.attributes.get("href", "")
                ]
                return urls if urls else []
        except asyncio.TimeoutError:
            print(f"Timeout fetching game URLs for {match_url} (attempt {attempt + 1}/{retries})")
            if attempt < retries - 1:
                await asyncio.sleep(2)
            continue
        except Exception as e:
            print(f"Failed to get game URLs for {match_url}: {e}")
            return []
    return []


async def fetch_tournament_matches(session, tournament, semaphore):
    encoded = quote(tournament)
    url = f"https://gol.gg/tournament/tournament-matchlist/{encoded}/" or ""
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://gol.gg/tournament-matchlist/",
        "Accept-Language": "en-US,en;q=0.9",
    }

    html = await fetch(session, url, semaphore, headers)
    if not html:
        return []

    tree = LexborHTMLParser(html)
    table = tree.css_first("table.table_list")
    if not table:
        print(f"No matches table found for {tournament}")
        return []

    matches = []
    rows = table.css("tr")[1:]

    for row in rows:
        cols = row.css("td")
        if len(cols) < 7:
            continue

        link_tag = cols[0].css_first("a")
        if not link_tag:
            continue

        match_url = (link_tag.attributes.get("href", "").strip().replace("page-game", "page-summary")
                     .replace("page-preview", "page-summary"))

        if not match_url.startswith("http"):
            match_url = "https://gol.gg/game/stats/" + match_url.split("/game/stats/")[-1]

        match_name = link_tag.text().strip()
        team1_name = cols[1].text().strip()
        score = cols[2].text().strip()
        team2_name = cols[3].text().strip()
        match_type = cols[4].text().strip()
        patch = cols[5].text().strip()
        match_date = cols[6].text().strip()

        team1_class = cols[1].attributes.get("class", "")
        team2_class = cols[3].attributes.get("class", "")

        if "text_victory" in team1_class:
            winner, loser = team1_name, team2_name
        elif "text_victory" in team2_class:
            winner, loser = team2_name, team1_name
        else:
            winner = loser = None

        bo = get_bo_(score)
        game_urls = await get_game_url(session, match_url, score)

        matches.append({
            "match_name": match_name,
            "tournament": tournament,
            "match_url": match_url,
            "team1": team1_name,
            "team2": team2_name,
            "winner": winner,
            "loser": loser,
            "score": score,
            "match_type": match_type,
            "patch": patch,
            "date": match_date,
            "BO": bo,
            "game_urls": game_urls,
        })

    await asyncio.sleep(0.5)
    return matches


async def get_matches_async(tournaments, max_concurrent=5):
    semaphore = asyncio.Semaphore(max_concurrent)
    results = []
    start = time.time()

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_tournament_matches(session, t, semaphore) for t in tournaments]
        tournaments_results = await asyncio.gather(*tasks)

        for tournament, matches in zip(tournaments, tournaments_results):
            print(f"{tournament}: {len(matches)} matches")
            results.extend(matches)

    print(f"Finished in {time.time() - start:.2f}s")
    return results

