import requests
import pandas as pd
import numpy as np
import io
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
import matplotlib.pyplot as plt
from joypy import joyplot

headers  = {
    'Connection': 'keep-alive',
    'Accept': 'application/json, text/plain, */*',
    'x-nba-stats-token': 'true',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
    'x-nba-stats-origin': 'stats',
    'Sec-Fetch-Site': 'same-origin',
    'Sec-Fetch-Mode': 'cors',
    'Referer': 'https://stats.nba.com/',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.9',
}

# play_by_play_url = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_0042000404.json"
# response = requests.get(url=play_by_play_url, headers=headers).json()
# play_by_play = response['game']['actions']
# df = pd.DataFrame(play_by_play)

# get game logs from the reg season
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2023-24',
                                              league_id_nullable='00',
                                              season_type_nullable='Regular Season')
games = gamefinder.get_data_frames()[0]
# Get a list of distinct game ids
game_ids = games['GAME_ID'].unique().tolist()
# create function that gets pbp logs from the 2020-21 season
def get_data(game_id):
    play_by_play_url = "https://cdn.nba.com/static/json/liveData/playbyplay/playbyplay_"+game_id+".json"
    response = requests.get(url=play_by_play_url, headers=headers).json()
    play_by_play = response['game']['actions']
    df = pd.DataFrame(play_by_play)
    df['gameid'] = game_id
    return df

# get data from all ids (takes awhile)
pbpdata = []
for game_id in game_ids:
    game_data = get_data(game_id)
    pbpdata.append(game_data)
df = pd.concat(pbpdata, ignore_index=True)
df.to_csv('pbp_data.csv', index=False)
df = pd.read_csv('pbp_data.csv')

df = df.sort_values(by=['gameid', 'orderNumber'])

# Convert score columns to numeric to avoid TypeError during subtraction
df['scoreHome'] = pd.to_numeric(df['scoreHome'])
df['scoreAway'] = pd.to_numeric(df['scoreAway'])

team_abbr = games['TEAM_ABBREVIATION'].unique()

# Initialize an empty DataFrame to store the results for all teams
all_teams_merged_df = pd.DataFrame()

for team in team_abbr:
    # Filter the games DataFrame for the current team
    games_filtered = games[games['TEAM_ABBREVIATION'] == team].copy()

    # Determine if the matchup is home or away
    games_filtered.loc[:, 'home_away'] = games_filtered['MATCHUP'].apply(lambda x: 'scoreHome' if 'vs.' in x else 'scoreAway')

    # Ensure matching data types for the merge operation
    df['gameid'] = df['gameid'].astype(str)
    games_filtered['GAME_ID'] = games_filtered['GAME_ID'].astype(str)
    #get rid of leading 0s in GAME_ID
    games_filtered['GAME_ID'] = games_filtered['GAME_ID'].str.lstrip('0')
    df['gameid'] = df['gameid'].str.lstrip('0')

    # Join the DataFrames on gameid and GAME_ID
    merged_df = pd.merge(df, games_filtered, left_on='gameid', right_on='GAME_ID')

    # Create the score_diff column based on the home_away determination
    merged_df['score_diff'] = merged_df.apply(lambda row: row['scoreHome'] - row['scoreAway'] if row['home_away'] == 'scoreHome' else row['scoreAway'] - row['scoreHome'], axis=1)

    # Append the results for this team to the all_teams_merged_df DataFrame
    all_teams_merged_df = pd.concat([all_teams_merged_df, merged_df], ignore_index=True)

# Display the final dataframe
print(merged_df.head())

# Assuming 'games' and 'merged_df' are your DataFrames
# Ensure 'merged_df' includes a 'TEAM_ABBREVIATION' column or join it with 'games' DataFrame if needed

# Convert 'clock' to seconds and calculate total seconds elapsed in the game for each possession
def clock_to_seconds(clock):
    minutes, seconds = clock.replace('PT', '').rstrip('S').split('M')
    return int(minutes) * 60 + float(seconds)


all_teams_merged_df['seconds_remaining'] = all_teams_merged_df['clock'].apply(clock_to_seconds)

SECONDS_PER_PERIOD = 12 * 60  # Assuming 12 minutes per period
all_teams_merged_df['total_seconds_elapsed'] = all_teams_merged_df.apply(
    lambda x: (x['period'] - 1) * SECONDS_PER_PERIOD + (SECONDS_PER_PERIOD - x['seconds_remaining']), axis=1)


# Function to calculate negative intervals
def calculate_negative_intervals(df):
    negative_intervals = []
    in_negative_period = False
    start_time = 0
    for index, row in df.iterrows():
        if row['score_diff'] <= -20 and not in_negative_period:
            in_negative_period = True
            start_time = row['total_seconds_elapsed']
        elif row['score_diff'] > -20 and in_negative_period:
            in_negative_period = False
            end_time = row['total_seconds_elapsed']
            negative_intervals.append((start_time, end_time))
    if in_negative_period:
        negative_intervals.append((start_time, df.iloc[-1]['total_seconds_elapsed']))
    return negative_intervals


# Get list of distinct TEAM_ABBREVIATIONs
distinct_teams = games['TEAM_ABBREVIATION'].unique()

# Initialize a list to store results
results = []

for team in distinct_teams:
    # Filter merged_df for current team
    team_df = all_teams_merged_df[all_teams_merged_df['TEAM_ABBREVIATION'] == team]

    # Calculate negative intervals for each game
    total_negative_time = 0
    for game_id in team_df['gameid'].unique():
        game_df = team_df[team_df['gameid'] == game_id]
        negative_intervals = calculate_negative_intervals(game_df)
        # Sum the durations of all negative intervals for the game
        for interval in negative_intervals:
            total_negative_time += interval[1] - interval[0]

    # Add result for current team to the results list
    total_negative_minutes = total_negative_time / 60  # Convert seconds to minutes
    results.append({'TEAM_ABBREVIATION': team, 'Total Negative Time (minutes)': total_negative_minutes})

# Load results into a new DataFrame
results_df = pd.DataFrame(results)

print(results_df)
