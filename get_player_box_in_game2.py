import requests
import pandas as pd
import numpy as np
import io
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
import requests
import pandas as pd
from openpyxl import load_workbook
import sqlite3  # Import sqlite3 module

# get game logs from the reg season
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25',
                                              league_id_nullable='00',
                                              season_type_nullable='Regular Season')
games = gamefinder.get_data_frames()[0]
# Get a list of distinct game ids
game_ids = games['GAME_ID'].unique().tolist()

# Filter to TEAM_ABBREVIATION == 'MIN' and get the game ids
games = games[games['TEAM_ABBREVIATION'] == 'MIN']
game_ids = games['GAME_ID'].unique().tolist()

# Grab the most recent game
game_id = game_ids[0]


def fetch_nba_data(url, headers):
    response = requests.get(url, headers=headers)
    data = response.json()
    return data

def process_data(data):
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    return df

# Common headers and URL
url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
    "If-Modified-Since": "Tue, 05 Mar 2024 05:22:46 GMT",
    "If-None-Match": "\"b1614aabd398e87ca9e79f1f64eef0f7\"",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"macOS\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

# Function to create a DataFrame from team data
def create_team_stats_df(team_data):
    # Initialize a list to hold player data
    players_data = []

    # Loop through each player and collect their statistics
    for player in team_data['players']:
        player_stats = player['statistics']
        player_info = {
            "Player": f"{player['firstName']} {player['familyName']}",
            "Position": player['position'],
        }
        # Merge player info with their statistics
        player_info.update(player_stats)

        # Add to the players_data list
        players_data.append(player_info)

    # Convert the list of player data into a DataFrame
    df = pd.DataFrame(players_data)
    return df

data = fetch_nba_data(url, headers)
# Extract the away team players' stats (assuming 'awayTeam' is 'MIN')
players_data = data["game"]["homeTeam"]["players"]

data_for_df = []
for player in players_data:
    player_info = {
        'Player': f"{player['firstName']} {player['familyName']}",
        'PTS': player['statistics']['points'],
        'REB': player['statistics']['reboundsTotal'],
        'AST': player['statistics']['assists'],
        'STL': player['statistics']['steals'],
        'TOV': player['statistics']['turnovers'],
        'BLK': player['statistics']['blocks'],
        'FGs': f"{player['statistics']['fieldGoalsMade']}-{player['statistics']['fieldGoalsAttempted']}",
        '3pt': f"{player['statistics']['threePointersMade']}-{player['statistics']['threePointersAttempted']}",
        'plusMinusPoints': player['statistics']['plusMinusPoints']
    }

    # Append to the data list
    data_for_df.append(player_info)

# Create DataFrame
df = pd.DataFrame(data_for_df)

# Simulated user selections
players_selected = df['Player'].tolist()
stats_selected = ['PTS', 'REB', 'AST', 'BLK', 'STL', 'TOV', 'FGs', '3pt', 'plusMinusPoints']

# Database operations
# Connect to SQLite database (it will create the database file if it doesn't exist)
conn = sqlite3.connect('/Users/tonysantoorjian/Documents/ww_db.db')
cursor = conn.cursor()

# Create the table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_stats (
        Player TEXT,
        PTS TEXT,
        REB TEXT,
        AST TEXT,
        BLK TEXT,
        STL TEXT,
        TOV TEXT,
        FGs TEXT,
        threePt TEXT,
        plusMinusPoints TEXT
    )
''')

# Clear any existing data in the table
cursor.execute('DELETE FROM player_stats')

# Insert data into the table
for _, row in df.iterrows():
    cursor.execute('''
        INSERT INTO player_stats (Player, PTS, REB, AST, BLK, STL, TOV, FGs, threePt, plusMinusPoints)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', tuple(row))

# Commit changes and close the connection
conn.commit()
conn.close()
