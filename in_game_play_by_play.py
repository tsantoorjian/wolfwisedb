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

# Ensure we have exactly 5 starters per team
if len(home_starters) != 5 or len(away_starters) != 5:
    print(f"Warning: Incorrect number of starters detected - Home: {len(home_starters)}, Away: {len(away_starters)}")

# Initialize lineups
current_lineups = {
    home_team_id: set(home_starters),
    away_team_id: set(away_starters)
}

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

# Convert player IDs to names using player_id_to_name dictionary
current_home_lineup_str = ', '.join([player_id_to_name.get(player_id, "Unknown Player") for player_id in current_home_lineup])
current_away_lineup_str = ', '.join([player_id_to_name.get(player_id, "Unknown Player") for player_id in current_away_lineup])

# Initialize the data for intervals
intervals_data = []

# Set initial values for new lineup strings
new_home_lineup_str = current_home_lineup_str
new_away_lineup_str = current_away_lineup_str

# Initialize interval scores and start time
interval_home_score_start = int(df_pbp.iloc[0]['scoreHome'])
interval_away_score_start = int(df_pbp.iloc[0]['scoreAway'])
interval_start_time = df_pbp.iloc[0]['timeActual']

# Iterate over the play-by-play data
# Process play-by-play data
for index, row in df_pbp.iterrows():
    action_type = row['actionType']
    sub_type = row['subType']
    person_id = row['personId']
    current_time = row['timeActual']

    # Update scores
    current_home_score = int(row['scoreHome'])
    current_away_score = int(row['scoreAway'])

    # Start of a free throw sequence
    if is_start_of_free_throw(row):
        free_throw_in_progress = True

    if free_throw_in_progress:
        if action_type == 'substitution':
            # Add any substitutions to the pending list until the free throw sequence is complete
            pending_substitutions.append(row)
        elif is_end_of_free_throw(row):
            # Free throw sequence ends here; apply all pending substitutions afterward
            free_throw_in_progress = False

            # Apply pending substitutions now
            for sub_row in pending_substitutions:
                sub_person_id = sub_row['personId']
                team_id = player_id_to_team_id.get(sub_person_id)
                if team_id is None:
                    continue

                # Update lineups based on team and action type (in or out)
                if team_id == home_team_id:
                    if sub_row['subType'] == 'out':
                        if sub_person_id in current_home_lineup:
                            current_home_lineup.remove(sub_person_id)
                    elif sub_row['subType'] == 'in':
                        if sub_person_id not in current_home_lineup:
                            current_home_lineup.append(sub_person_id)
                elif team_id == away_team_id:
                    if sub_row['subType'] == 'out':
                        if sub_person_id in current_away_lineup:
                            current_away_lineup.remove(sub_person_id)
                    elif sub_row['subType'] == 'in':
                        if sub_person_id not in current_away_lineup:
                            current_away_lineup.append(sub_person_id)

            # Clear pending substitutions after applying
            pending_substitutions = []

            # Update lineups and calculate interval plus-minus after free throws
            new_home_lineup_str = ', '.join([player_id_to_name.get(pid, "Unknown Player") for pid in current_home_lineup])
            new_away_lineup_str = ', '.join([player_id_to_name.get(pid, "Unknown Player") for pid in current_away_lineup])

            # Process interval if lineups have changed
            if new_home_lineup_str != current_home_lineup_str or new_away_lineup_str != current_away_lineup_str:
                # Calculate interval scores
                interval_home_score_end = current_home_score
                interval_away_score_end = current_away_score

                interval_home_score = interval_home_score_end - interval_home_score_start
                interval_away_score = interval_away_score_end - interval_away_score_start

                # Append interval data
                intervals_data.append({
                    'Start Time': interval_start_time,
                    'End Time': current_time,
                    'Home Lineup': current_home_lineup_str,
                    'Away Lineup': current_away_lineup_str,
                    'Home Score': interval_home_score,
                    'Away Score': interval_away_score,
                    'Plus/Minus': interval_home_score - interval_away_score
                })

                # Reset for the next interval
                interval_start_time = current_time
                interval_home_score_start = current_home_score
                interval_away_score_start = current_away_score

                # Update the current lineup strings
                current_home_lineup_str = new_home_lineup_str
                current_away_lineup_str = new_away_lineup_str

    else:
        # If no free throws are in progress, process substitutions immediately
        if action_type == 'substitution':
            team_id = player_id_to_team_id.get(person_id)
            if team_id is None:
                continue

            # Update lineups immediately for substitutions outside free throw sequences
            if team_id == home_team_id:
                if sub_type == 'out':
                    if person_id in current_home_lineup:
                        current_home_lineup.remove(person_id)
                elif sub_type == 'in':
                    if person_id not in current_home_lineup:
                        current_home_lineup.append(person_id)
            elif team_id == away_team_id:
                if sub_type == 'out':
                    if person_id in current_away_lineup:
                        current_away_lineup.remove(person_id)
                elif sub_type == 'in':
                    if person_id not in current_away_lineup:
                        current_away_lineup.append(person_id)

            # Update lineup strings
            new_home_lineup_str = ', '.join([player_id_to_name.get(pid, "Unknown Player") for pid in current_home_lineup])
            new_away_lineup_str = ', '.join([player_id_to_name.get(pid, "Unknown Player") for pid in current_away_lineup])

            # Process interval if lineups have changed
            if new_home_lineup_str != current_home_lineup_str or new_away_lineup_str != current_away_lineup_str:
                # Calculate interval scores
                interval_home_score_end = current_home_score
                interval_away_score_end = current_away_score

                interval_home_score = interval_home_score_end - interval_home_score_start
                interval_away_score = interval_away_score_end - interval_away_score_start

                # Append interval data
                intervals_data.append({
                    'Start Time': interval_start_time,
                    'End Time': current_time,
                    'Home Lineup': current_home_lineup_str,
                    'Away Lineup': current_away_lineup_str,
                    'Home Score': interval_home_score,
                    'Away Score': interval_away_score,
                    'Plus/Minus': interval_home_score - interval_away_score
                })

                # Reset for the next interval
                interval_start_time = current_time
                interval_home_score_start = current_home_score
                interval_away_score_start = current_away_score

                # Update the current lineup strings
                current_home_lineup_str = new_home_lineup_str
                current_away_lineup_str = new_away_lineup_str

# Final interval if needed
if index == df_pbp.index[-1]:
    interval_home_score_end = current_home_score
    interval_away_score_end = current_away_score

    interval_home_score = interval_home_score_end - interval_home_score_start
    interval_away_score = interval_away_score_end - interval_away_score_start

    intervals_data.append({
        'Start Time': interval_start_time,
        'End Time': current_time,
        'Home Lineup': current_home_lineup_str,
        'Away Lineup': current_away_lineup_str,
        'Home Score': interval_home_score,
        'Away Score': interval_away_score,
        'Plus/Minus': interval_home_score - interval_away_score
    })

# Convert the intervals data to a DataFrame
intervals_df = pd.DataFrame(intervals_data)

# Save the DataFrame to an Excel file
intervals_df.to_excel('/Users/tonysantoorjian/Documents/lineup_intervals_plus_minus.xlsx', index=False)

df_pbp.to_csv('/Users/tonysantoorjian/Documents/pbp.csv', index=False)

# Prepare to add the columns for individual players and helper columns
# Split "Home Lineup" and "Away Lineup" columns into separate columns for each player

# Split the Home Lineup and Away Lineup into separate columns
home_lineup_split = intervals_df['Home Lineup'].str.split(', ', expand=True).rename(columns=lambda x: f'Home Player {x + 1}')
away_lineup_split = intervals_df['Away Lineup'].str.split(', ', expand=True).rename(columns=lambda x: f'Away Player {x + 1}')

# Concatenate the new player columns with the original DataFrame
intervals_df_expanded = pd.concat([intervals_df, home_lineup_split, away_lineup_split], axis=1)

# Add an "All Players Present" helper column for user-selected players, to be populated later in Excel with a formula
intervals_df_expanded['All Players Present'] = ''

# Save the modified DataFrame back to an Excel file
output_path = '/Users/tonysantoorjian/Documents/lineup_intervals_plus_minus_expanded.xlsx'
intervals_df_expanded.to_excel(output_path, index=False)


