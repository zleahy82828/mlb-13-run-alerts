import os
import csv
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

TEAM_NAME_ALIASES = {
    "diamondbacks": "Arizona Diamondbacks",
    "arizona diamondbacks": "Arizona Diamondbacks",
    "braves": "Atlanta Braves",
    "atlanta braves": "Atlanta Braves",
    "orioles": "Baltimore Orioles",
    "baltimore orioles": "Baltimore Orioles",
    "red sox": "Boston Red Sox",
    "boston red sox": "Boston Red Sox",
    "cubs": "Chicago Cubs",
    "chicago cubs": "Chicago Cubs",
    "white sox": "Chicago White Sox",
    "chicago white sox": "Chicago White Sox",
    "reds": "Cincinnati Reds",
    "cincinnati reds": "Cincinnati Reds",
    "guardians": "Cleveland Guardians",
    "cleveland guardians": "Cleveland Guardians",
    "rockies": "Colorado Rockies",
    "colorado rockies": "Colorado Rockies",
    "tigers": "Detroit Tigers",
    "detroit tigers": "Detroit Tigers",
    "astros": "Houston Astros",
    "houston astros": "Houston Astros",
    "royals": "Kansas City Royals",
    "kansas city royals": "Kansas City Royals",
    "angels": "Los Angeles Angels",
    "los angeles angels": "Los Angeles Angels",
    "dodgers": "Los Angeles Dodgers",
    "los angeles dodgers": "Los Angeles Dodgers",
    "marlins": "Miami Marlins",
    "miami marlins": "Miami Marlins",
    "brewers": "Milwaukee Brewers",
    "milwaukee brewers": "Milwaukee Brewers",
    "twins": "Minnesota Twins",
    "minnesota twins": "Minnesota Twins",
    "mets": "New York Mets",
    "new york mets": "New York Mets",
    "yankees": "New York Yankees",
    "new york yankees": "New York Yankees",
    "athletics": "Athletics",
    "oakland athletics": "Athletics",
    "a's": "Athletics",
    "phillies": "Philadelphia Phillies",
    "philadelphia phillies": "Philadelphia Phillies",
    "pirates": "Pittsburgh Pirates",
    "pittsburgh pirates": "Pittsburgh Pirates",
    "padres": "San Diego Padres",
    "san diego padres": "San Diego Padres",
    "giants": "San Francisco Giants",
    "san francisco giants": "San Francisco Giants",
    "mariners": "Seattle Mariners",
    "seattle mariners": "Seattle Mariners",
    "cardinals": "St. Louis Cardinals",
    "st. louis cardinals": "St. Louis Cardinals",
    "rays": "Tampa Bay Rays",
    "tampa bay rays": "Tampa Bay Rays",
    "rangers": "Texas Rangers",
    "texas rangers": "Texas Rangers",
    "blue jays": "Toronto Blue Jays",
    "toronto blue jays": "Toronto Blue Jays",
    "nationals": "Washington Nationals",
    "washington nationals": "Washington Nationals",
}


def normalize_team_name(name: str) -> str:
    if not name:
        return ""
    key = name.strip().lower()
    return TEAM_NAME_ALIASES.get(key, name.strip())


def write_google_credentials_file():
    creds_json = os.environ["GOOGLE_SERVICE_ACCOUNT_JSON"]
    with open("google-service-account.json", "w", encoding="utf-8") as f:
        f.write(creds_json)


def get_gspread_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        "google-service-account.json",
        scopes=scopes
    )
    return gspread.authorize(creds)


def get_worksheet(sheet_id: str, worksheet_name: str):
    gc = get_gspread_client()
    sh = gc.open_by_key(sheet_id)
    return sh.worksheet(worksheet_name)


def load_csv_rows(url: str):
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    lines = response.text.splitlines()
    return list(csv.DictReader(lines))


def load_config():
    rows = load_csv_rows(os.environ["CONFIG_CSV_URL"])
    config = {}
    for row in rows:
        config[row["key"].strip()] = row["value"].strip()
    return config


def load_assignments():
    rows = load_csv_rows(os.environ["ASSIGNMENTS_CSV_URL"])
    assignments = []
    for row in rows:
        assignments.append({
            "week_start": row["week_start"].strip(),
            "team": normalize_team_name(row["team"]),
            "participant": row["participant"].strip()
        })
    return assignments


def get_today_in_tz(tz_name: str):
    return datetime.now(ZoneInfo(tz_name)).date()


def get_week_start(d):
    return d - timedelta(days=d.weekday())


def iso_date(d):
    return d.strftime("%Y-%m-%d")


def fetch_final_games_for_date(target_date):
    params = {
        "sportId": 1,
        "date": target_date.strftime("%Y-%m-%d"),
        "hydrate": "linescore,team"
    }
    response = requests.get(MLB_SCHEDULE_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    games = []
    for date_block in data.get("dates", []):
        for game in date_block.get("games", []):
            status = game.get("status", {}).get("detailedState", "")
            abstract_state = game.get("status", {}).get("abstractGameState", "")

            if abstract_state != "Final" and status not in ("Final", "Game Over"):
                continue

            teams = game.get("teams", {})
            home = teams.get("home", {})
            away = teams.get("away", {})

            games.append({
                "game_pk": str(game.get("gamePk")),
                "game_date": game.get("gameDate", ""),
                "home_team": normalize_team_name(home.get("team", {}).get("name", "")),
                "away_team": normalize_team_name(away.get("team", {}).get("name", "")),
                "home_runs": int(home.get("score", 0)),
                "away_runs": int(away.get("score", 0)),
            })
    return games


def find_13_run_results(games):
    results = []
    for game in games:
        if game["home_runs"] == 13:
            results.append({
                "game_pk": game["game_pk"],
                "game_date": game["game_date"],
                "team": game["home_team"],
                "opponent": game["away_team"],
                "team_runs": 13,
                "opponent_runs": game["away_runs"]
            })
        if game["away_runs"] == 13:
            results.append({
                "game_pk": game["game_pk"],
                "game_date": game["game_date"],
                "team": game["away_team"],
                "opponent": game["home_team"],
                "team_runs": 13,
                "opponent_runs": game["home_runs"]
            })
    return results


def get_participant_for_team(assignments, week_start_str, team_name):
    normalized_team = normalize_team_name(team_name)
    for row in assignments:
        if row["week_start"] == week_start_str and normalize_team_name(row["team"]) == normalized_team:
            return row["participant"]
    return None


def get_logged_keys(sheet_id):
    ws = get_worksheet(sheet_id, "ResultsLog")
    records = ws.get_all_records()
    keys = set()
    for r in records:
        game_pk = str(r.get("game_pk", "")).strip()
        team = normalize_team_name(str(r.get("team", "")).strip())
        if game_pk and team:
            keys.add(f"{game_pk}|{team}")
    return keys


def append_result_log(sheet_id, row):
    ws = get_worksheet(sheet_id, "ResultsLog")
    ws.append_row(row, value_input_option="USER_ENTERED")


def send_telegram_message(bot_token, chat_id, body):
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": body
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def build_alert_message(result, participant, week_start_str):
    return (
        f"13-Run Alert\n"
        f"Week of {week_start_str}\n"
        f"Team: {result['team']}\n"
        f"Owner: {participant if participant else 'NOT FOUND'}\n"
        f"Final: {result['team']} {result['team_runs']}, {result['opponent']} {result['opponent_runs']}"
    )


def send_test_alert():
    msg = (
        "TEST ALERT\n"
        "13-Run Baseball Pool notifier is working.\n"
        "This is a manual GitHub Actions test."
    )
    send_telegram_message(
        os.environ["TELEGRAM_BOT_TOKEN"],
        os.environ["TELEGRAM_CHAT_ID"],
        msg
    )
    print("Test Telegram alert sent.")


def run_live():
    sheet_id = os.environ["SPREADSHEET_ID"]
    config = load_config()
    assignments = load_assignments()

    tz_name = config.get("timezone", "America/New_York")
    today = get_today_in_tz(tz_name)
    week_start = get_week_start(today)
    week_start_str = iso_date(week_start)

    games = fetch_final_games_for_date(today)
    results = find_13_run_results(games)

    if not results:
        print("No final games with exactly 13 runs today.")
        return

    logged_keys = get_logged_keys(sheet_id)

    for result in results:
        participant = get_participant_for_team(assignments, week_start_str, result["team"])
        dedupe_key = f"{result['game_pk']}|{normalize_team_name(result['team'])}"

        if dedupe_key in logged_keys:
            print(f"Already logged: {dedupe_key}")
            continue

        msg = build_alert_message(result, participant, week_start_str)

        send_telegram_message(
            os.environ["TELEGRAM_BOT_TOKEN"],
            os.environ["TELEGRAM_CHAT_ID"],
            msg
        )

        timestamp = datetime.now(ZoneInfo(tz_name)).strftime("%Y-%m-%d %H:%M:%S")

        append_result_log(sheet_id, [
            timestamp,
            week_start_str,
            result["game_pk"],
            result["game_date"],
            result["team"],
            result["opponent"],
            result["team_runs"],
            result["opponent_runs"],
            participant if participant else "",
            "YES - Telegram"
        ])

        print(f"Telegram alert sent for {result['team']}")


def run_mock_live():
    sheet_id = os.environ["SPREADSHEET_ID"]
    config = load_config()
    assignments = load_assignments()

    tz_name = config.get("timezone", "America/New_York")
    today = get_today_in_tz(tz_name)
    week_start = get_week_start(today)
    week_start_str = iso_date(week_start)

    # CHANGE THESE VALUES TO MATCH A REAL CURRENT-WEEK ASSIGNMENT
    mock_result = {
        "game_pk": "MOCK-GAME-002",
        "game_date": f"{today}T23:00:00Z",
        "team": "New York Yankees",
        "opponent": "Boston Red Sox",
        "team_runs": 13,
        "opponent_runs": 4
    }

    participant = get_participant_for_team(assignments, week_start_str, mock_result["team"])
    dedupe_key = f"{mock_result['game_pk']}|{normalize_team_name(mock_result['team'])}"

    print(f"Today: {today}")
    print(f"Week start: {week_start_str}")
    print(f"Mock team: {mock_result['team']}")
    print(f"Participant found: {participant}")
    print(f"Dedupe key: {dedupe_key}")

    logged_keys = get_logged_keys(sheet_id)
    if dedupe_key in logged_keys:
        print(f"Already logged: {dedupe_key}")
        return

    msg = build_alert_message(mock_result, participant, week_start_str)

    send_telegram_message(
        os.environ["TELEGRAM_BOT_TOKEN"],
