import pandas as pd

# Sample data setup (replace with your actual data)
df = pd.DataFrame({
    'gameid': ['1', '2', '3'],
    'scoreHome': [100, 110, 120],
    'scoreAway': [90, 105, 115]
})

games = pd.DataFrame({
    'TEAM_ABBREVIATION': ['MIN', 'BOS', 'MIN'],
    'GAME_ID': ['1', '2', '3'],
    'MATCHUP': ['MIN vs. XYZ', 'BOS @ XYZ', 'MIN vs. XYZ']
})

# Get list of unique team abbreviations
team_abbr = games['TEAM_ABBREVIATION'].unique()

# Initialize an empty DataFrame to store the results for all teams
all_teams_merged_df = pd.DataFrame()

for team in team_abbr:
    # Filter the games DataFrame for the current team
    games_filtered = games[games['TEAM_ABBREVIATION'] == team].copy()

    # Diagnostic check: Print if games_filtered is empty
    if games_filtered.empty:
        print(f"No games found for team: {team}")
        continue

    # Determine if the matchup is home or away
    games_filtered.loc[:, 'home_away'] = games_filtered['MATCHUP'].apply(
        lambda x: 'scoreHome' if 'vs.' in x else 'scoreAway')

    # Ensure matching data types for the merge operation
    df['gameid'] = df['gameid'].astype(str).str.strip()
    games_filtered['GAME_ID'] = games_filtered['GAME_ID'].astype(str).str.strip()

    # Join the DataFrames on gameid and GAME_ID
    merged_df = pd.merge(df, games_filtered, left_on='gameid', right_on='GAME_ID')

    # Diagnostic check: Print if merged_df is empty after the join
    if merged_df.empty:
        print(f"Merge resulted in an empty DataFrame for team: {team}")
        continue

    # Create the score_diff column based on the home_away determination
    merged_df['score_diff'] = merged_df.apply(
        lambda row: row['scoreHome'] - row['scoreAway'] if row['home_away'] == 'scoreHome' else row['scoreAway'] - row[
            'scoreHome'], axis=1)

    # Append the results for this team to the all_teams_merged_df DataFrame
    all_teams_merged_df = pd.concat([all_teams_merged_df, merged_df], ignore_index=True)

# Check if the final DataFrame is empty
if all_teams_merged_df.empty:
    print("Final DataFrame is empty. Check the input DataFrames and join conditions.")
else:
    print(all_teams_merged_df.head())
