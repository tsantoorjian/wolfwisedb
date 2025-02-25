import requests
import pandas as pd
import numpy as np
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
import os
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = 'https://kuthirbcjtofsdwsfhkj.supabase.co'
supabase_key = os.getenv('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)

# Get game logs from the reg season
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25',
                                              league_id_nullable='00',
                                              season_type_nullable='Regular Season')
games = gamefinder.get_data_frames()[0]

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

def save_to_supabase(df, table_name="in_game_player_stats"):
    try:
        # Convert DataFrame to list of dictionaries for Supabase
        records = df.to_dict('records')
        
        # Delete existing records - need to use a WHERE clause
        # Using a condition that will match all records
        supabase.table(table_name).delete().neq("Player", "NO_SUCH_PLAYER").execute()
        
        # Insert new records
        result = supabase.table(table_name).insert(records).execute()
        
        print(f"Successfully saved in-game stats to {table_name}")
        
        # Print preview of the data
        print(f"\nPreview of in-game data:")
        print(df[['Player', 'PTS', 'REB', 'AST']].head())
        
        # Print the opponent team
        if 'home_team' in globals() and 'away_team' in globals():
            if home_team["teamTricode"] == "MIN":
                opponent = away_team["teamTricode"]
            else:
                opponent = home_team["teamTricode"]
            print(f"Opponent: {opponent}")
        
    except Exception as e:
        print(f"Error saving in-game stats to {table_name}: {str(e)}")
        # Print more detailed error information
        import traceback
        traceback.print_exc()

def save_to_csv(df, output_dir='in_game_stats'):
    """Save the in-game stats to a CSV file"""
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Generate filename with timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = os.path.join(output_dir, f'in_game_stats_{timestamp}.csv')
    
    # Save to CSV
    df.to_csv(filename, index=False)
    print(f"In-game stats saved to CSV: {filename}")
    
    return filename

# Main execution
try:
    # Fetch data
    data = fetch_nba_data(url, headers)
    
    # Determine which team is the Timberwolves
    home_team = data["game"]["homeTeam"]
    away_team = data["game"]["awayTeam"]
    
    # Select the Timberwolves team data
    if home_team["teamTricode"] == "MIN":
        wolves_team = home_team
        print("Timberwolves are the home team")
    elif away_team["teamTricode"] == "MIN":
        wolves_team = away_team
        print("Timberwolves are the away team")
    else:
        raise ValueError("Minnesota Timberwolves (MIN) not found in this game data")
    
    # Extract the Timberwolves players' stats
    players_data = wolves_team["players"]
    
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
            'threePt': f"{player['statistics']['threePointersMade']}-{player['statistics']['threePointersAttempted']}",
            'plusMinusPoints': player['statistics']['plusMinusPoints']
        }
    
        # Append to the data list
        data_for_df.append(player_info)
    
    # Create DataFrame
    df = pd.DataFrame(data_for_df)
    
    # Save to Supabase
    save_to_supabase(df)
    
    # Also save to CSV for testing
    #csv_file = save_to_csv(df)
    
except Exception as e:
    print(f"Error processing in-game stats: {str(e)}")