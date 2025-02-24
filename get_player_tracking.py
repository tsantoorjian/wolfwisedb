import requests
import pandas as pd
import sqlite3
import time


def process_data(data):
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    return df


# The URLs and parameters as defined before
url_tracking_stats = "https://stats.nba.com/stats/leaguedashptstats"
url_playtype_stats = "https://www.nba.com/_next/data/b02IEPqxZ0y-px1JNxnyJ/en/stats/players"

# Headers to mimic a request from a web browser
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
}

# Define play types, tracking types, and other required parameters
play_types = ['OffScreen', 'Isolation', 'Transition', 'Postup', 'Spotup', 'Handoff', 'Cut', 'OffRebound', 'Misc',
              'PRRollman', 'PRBallHandler']
tracking_types = ['Drives', 'Defense', 'CatchShoot', 'Passing', 'Possessions', 'PullUpShot', 'Rebounding', 'Efficiency',
                  'SpeedDistance', 'ElbowTouch', 'PostTouch', 'PaintTouch']
last_n_games_values = ['0', '5', '10']
player_or_team_values_playtype = ['P', 'T']
player_or_team_values_tracking = ['Player', 'Team']

# Parameters template for playtype stats
params_playtype_stats = {
    "LeagueID": "00",
    "SeasonYear": "2024-25",
    "SeasonType": "Regular Season",
    "PerMode": "Totals",
    "TypeGrouping": "offensive"
}

# Parameters template for tracking stats
params_tracking_stats = {
    "College": "",
    "Conference": "",
    "Country": "",
    "DateFrom": "",
    "DateTo": "",
    "Division": "",
    "DraftPick": "",
    "DraftYear": "",
    "GameScope": "",
    "Height": "",
    "ISTRound": "",
    "LastNGames": 0,
    "LeagueID": "00",
    "Location": "",
    "Month": 0,
    "OpponentTeamID": 0,
    "Outcome": "",
    "PORound": 0,
    "PerMode": "Totals",
    "PlayerExperience": "",
    "PlayerOrTeam": "Player",
    "PlayerPosition": "",
    "PtMeasureType": "Drives",
    "Season": "2024-25",
    "SeasonSegment": "",
    "SeasonType": "Regular Season",
    "StarterBench": "",
    "TeamID": 0,
    "VsConference": "",
    "VsDivision": "",
    "Weight": ""
}

# Establish a connection to the SQLite database
conn = sqlite3.connect('/Users/tonysantoorjian/Documents/ww_db.db')

# Loop through play types
for play_type in play_types:
    for player_or_team in player_or_team_values_playtype:
        df_list = []
        for last_n_games in last_n_games_values:
            params_playtype_stats['PtMeasureType'] = play_type
            params_playtype_stats['LastNGames'] = last_n_games
            params_playtype_stats['PlayerOrTeam'] = player_or_team

            # Print details of the current request
            print(
                f"Requesting play type data: play_type={play_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")

            # Make the request
            response = requests.get(url_playtype_stats, headers=headers, params=params_playtype_stats)

            # Check response status before proceeding
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'resultSets' in data and len(data['resultSets']) > 0:
                        df = process_data(data)
                        df['LastNGames'] = last_n_games
                        df['PLAYER_OR_TEAM'] = player_or_team
                        df_list.append(df)
                    else:
                        print(
                            f"No data available for play_type={play_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")
                except requests.exceptions.JSONDecodeError:
                    print(
                        f"JSON decode error for play_type={play_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")
            else:
                print(
                    f"Request failed with status code {response.status_code} for play_type={play_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")
                print(response.text)

            time.sleep(1)  # Add delay to avoid being blocked

        if df_list:
            df_concat = pd.concat(df_list, ignore_index=True)
            df_concat.to_sql(f"{play_type}_{player_or_team}", conn, if_exists='replace', index=False)

# Repeat for tracking types
for tracking_type in tracking_types:
    for player_or_team in player_or_team_values_tracking:
        df_list = []
        for last_n_games in last_n_games_values:
            params_tracking_stats['PtMeasureType'] = tracking_type
            params_tracking_stats['LastNGames'] = last_n_games
            params_tracking_stats['PlayerOrTeam'] = player_or_team

            # Print details of the current request
            print(
                f"Requesting tracking type data: tracking_type={tracking_type}, player_or_team={player_or_team}, last_n_games={last_n_games}, Attempting to fetch data from URL:", url_tracking_stats)

            response = requests.get(url_tracking_stats, headers=headers, params=params_tracking_stats)

            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'resultSets' in data and len(data['resultSets']) > 0:
                        df = process_data(data)
                        df['LastNGames'] = last_n_games
                        df['PLAYER_OR_TEAM'] = player_or_team
                        df_list.append(df)
                    else:
                        print(
                            f"No data available for tracking_type={tracking_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")
                except requests.exceptions.JSONDecodeError:
                    print(
                        f"JSON decode error for tracking_type={tracking_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")
            else:
                print(
                    f"Request failed with status code {response.status_code} for tracking_type={tracking_type}, player_or_team={player_or_team}, last_n_games={last_n_games}")
                print(response.text)

            time.sleep(1)

        if df_list:
            df_concat = pd.concat(df_list, ignore_index=True)
            df_concat.to_sql(f"{tracking_type}_{player_or_team}", conn, if_exists='replace', index=False)

conn.close()
