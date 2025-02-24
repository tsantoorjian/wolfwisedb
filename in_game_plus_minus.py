import pandas as pd
import requests
import pandas as pd
from collections import defaultdict
from nba_api.stats.endpoints import leaguegamefinder
import time
import re


# Function to fetch data
def fetch_nba_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 429:
            print(f"Rate limit exceeded (status 429) for URL: {url}")
            print(f"Retry-After: {response.headers.get('Retry-After', 'not provided')}")
            time.sleep(10)  # Wait before retrying
            return fetch_nba_data(url, headers)  # Retry the request
        response.raise_for_status()  # Will raise an HTTPError if status is not 200-299
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None


# Function to convert clock string to seconds (e.g., 'PT06M06.00S' -> 366 seconds)
def parse_clock(clock_str):
    import re
    match = re.match(r'PT(\d+)M(\d+\.\d+)S', clock_str)
    if match:
        minutes = int(match.group(1))
        seconds = float(match.group(2))
        return minutes * 60 + seconds
    else:
        return 0  # Return 0 if clock_str is not valid

def is_start_of_free_throw(action):
    """Check if this action is the start of a free throw sequence (e.g., '1 of 2')."""
    return action['actionType'] == 'freethrow' and action['subType'] and '1 of' in action['subType']

def is_end_of_free_throw(action):
    """Check if this action is the end of a free throw sequence (e.g., '2 of 2')."""
    if action['actionType'] != 'freethrow':
        return False
    sub_type = action['subType']
    if sub_type is None:
        return True  # End if no subType available
    match = re.match(r'(\d+) of (\d+)', sub_type)
    if match:
        shot_number = int(match.group(1))
        total_shots = int(match.group(2))
        return shot_number == total_shots
    return True  # Assume it's the last free throw


free_throw_in_progress = False
pending_substitutions = []


# Get game logs from the regular season
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25',
                                               league_id_nullable='00',
                                               season_type_nullable='Regular Season')
games = gamefinder.get_data_frames()[0]

# Filter to TEAM_ABBREVIATION == 'MIN' and get the game ids
games = games[games['TEAM_ABBREVIATION'] == 'MIN']
game_ids = games['GAME_ID'].unique().tolist()

# Grab the most recent game
game_id = game_ids[0]
#game_id = '0022400076'
# URLs
pbp_url = f"https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_{game_id}.json"
boxscore_url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"

headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "User-Agent": "Mozilla/5.0"
}

# Fetch data
pbp_data = fetch_nba_data(pbp_url, headers)
boxscore_data = fetch_nba_data(boxscore_url, headers)

# Access team information from boxscore_data
home_team = boxscore_data['game'].get('homeTeam', {})
away_team = boxscore_data['game'].get('awayTeam', {})

# Check if homeTeam and awayTeam exist
if not home_team or not away_team:
    print("Team data not found in boxscore_data.")
    exit()

home_team_id = home_team['teamId']
away_team_id = away_team['teamId']

# Create player ID to name and team ID mappings
player_id_to_name = {}
player_id_to_team_id = {}
for player in home_team.get('players', []):
    person_id = player['personId']
    player_name = f"{player['firstName']} {player['familyName']}"
    player_id_to_name[person_id] = player_name
    player_id_to_team_id[person_id] = home_team_id  # Map player to home team ID

for player in away_team.get('players', []):
    person_id = player['personId']
    player_name = f"{player['firstName']} {player['familyName']}"
    player_id_to_name[person_id] = player_name
    player_id_to_team_id[person_id] = away_team_id  # Map player to away team ID

# Get the 5 starters from each team based on the 'starter' flag
home_starters = [player['personId'] for player in home_team['players'] if player.get('starter')][:5]
away_starters = [player['personId'] for player in away_team['players'] if player.get('starter')][:5]
home_team = [player_id_to_name.get(player_id, "Unknown Player") for player_id in home_starters]
away_team = [player_id_to_name.get(player_id, "Unknown Player") for player_id in away_starters]


# Process play-by-play actions
actions = pbp_data['game']['actions']

# Convert actions to DataFrame
df_pbp = pd.DataFrame(actions)

# Ensure actions are sorted chronologically by period, timeActual, and clock
df_pbp = df_pbp.sort_values(['period', 'clock'], ascending=[True, False]).reset_index(drop=True)
df_pbp.to_csv('/Users/tonysantoorjian/Documents/pbp.csv', index=False)
pbp_data = pd.read_csv('/Users/tonysantoorjian/Documents/pbp.csv')

# Reinitialize the tracking variables
current_home_lineup = home_starters.copy()
current_away_lineup = away_starters.copy()

# Load the play-by-play data
file_path = '/Users/tonysantoorjian/Documents/pbp.csv'
pbp_data = pd.read_csv(file_path)

# Update all substitution action types to "zsubstitution" to ensure they are sorted last
pbp_data['actionType'] = pbp_data['actionType'].replace('substitution', 'zsubstitution')

# Sort by period ascending, clock descending, and actionType ascending
pbp_data_sorted = pbp_data.sort_values(by=['period', 'clock', 'actionType'], ascending=[True, False, True]).reset_index(
    drop=True)


# Initialize starting lineups again
home_lineup = home_team.copy()
away_lineup = away_team.copy()

# Track substitutions with pairs of 'in' and 'out'
substitutions = {'home': [], 'away': []}

# Create columns to store current lineups
pbp_data_sorted['homeplayer1'] = ""
pbp_data_sorted['homeplayer2'] = ""
pbp_data_sorted['homeplayer3'] = ""
pbp_data_sorted['homeplayer4'] = ""
pbp_data_sorted['homeplayer5'] = ""
pbp_data_sorted['awayplayer1'] = ""
pbp_data_sorted['awayplayer2'] = ""
pbp_data_sorted['awayplayer3'] = ""
pbp_data_sorted['awayplayer4'] = ""
pbp_data_sorted['awayplayer5'] = ""

# Iterate through each row in the sorted play-by-play data
for index, row in pbp_data_sorted.iterrows():
    action_type = row['actionType']
    person_id = row['personId']
    sub_type = row['subType']
    current_home_score = row.get('scoreHome', 0)
    current_away_score = row.get('scoreAway', 0)

    # Handle substitution events
    if action_type == 'zsubstitution':
        player_name = player_id_to_name.get(person_id)
        team_id = player_id_to_team_id.get(person_id)

        if sub_type == 'in':
            if team_id == 1610612750:
                # Home team substitution in
                substitutions['home'].append(('in', player_name))
            elif team_id == 1610612742:
                # Away team substitution in
                substitutions['away'].append(('in', player_name))
        elif sub_type == 'out':
            if team_id == 1610612750:
                # Home team substitution out
                substitutions['home'].append(('out', player_name))
            elif team_id == 1610612742:
                # Away team substitution out
                substitutions['away'].append(('out', player_name))

        # Process substitutions to ensure lineups always have 5 players
        for team in ['home', 'away']:
            if len(substitutions[team]) >= 2:
                # Expecting a pair of 'out' and 'in'
                out_player = [sub for sub in substitutions[team] if sub[0] == 'out']
                in_player = [sub for sub in substitutions[team] if sub[0] == 'in']
                if out_player and in_player:
                    out_player_name = out_player[0][1]
                    in_player_name = in_player[0][1]
                    substitutions[team] = [sub for sub in substitutions[team] if
                                           sub != out_player[0] and sub != in_player[0]]

                    if team == 'home':
                        # Remove player who was subbed out and add the player who subbed in, with safeguard
                        if out_player_name in home_lineup:
                            home_lineup.remove(out_player_name)
                        home_lineup.append(in_player_name)
                    elif team == 'away':
                        # Remove player who was subbed out and add the player who subbed in, with safeguard
                        if out_player_name in away_lineup:
                            away_lineup.remove(out_player_name)
                        away_lineup.append(in_player_name)

    # Update the current lineups for the row
    for i in range(5):
        pbp_data_sorted.at[index, f'homeplayer{i + 1}'] = home_lineup[i]
        pbp_data_sorted.at[index, f'awayplayer{i + 1}'] = away_lineup[i]

# Initialize new column for plus-minus calculation
pbp_data_sorted['plus_minus'] = 0

# Iterate through each row in the play-by-play data to calculate plus-minus
for index, row in pbp_data_sorted.iterrows():
    action_type = row['actionType']
    shot_result = row.get('shotResult', None)
    team_id = row.get('teamId', None)

    # Ensure the shot result is properly compared and team_id is interpreted as an integer
    if action_type in ['2pt', '3pt', 'freethrow'] and shot_result == 'Made':
        if action_type == '2pt':
            value = 2
        elif action_type == '3pt':
            value = 3
        elif action_type == 'freethrow':
            value = 1

        # Round team_id to eliminate scientific notation effect
        team_id = int(round(team_id)) if pd.notna(team_id) else None

        if team_id == 1610612750:  # Home team
            pbp_data_sorted.at[index, 'plus_minus'] = value
        elif team_id == 1610612743:  # Away team
            pbp_data_sorted.at[index, 'plus_minus'] = -value

# Save the updated play-by-play data with plus-minus calculations
pbp_data_sorted.to_excel('/Users/tonysantoorjian/Documents/pbp_with_plus_minus_corrected.xlsx', index=False)