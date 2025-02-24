#!/usr/bin/env python
import datetime
import pandas as pd
from nba_api.stats.endpoints import leaguedashplayerstats

def get_current_season():
    """
    Returns the NBA season string (e.g., "2024-25") based on the current date.
    NBA seasons typically start in October.
    """
    now = datetime.datetime.now()
    if now.month >= 10:
        season_start = now.year
        season_end = now.year + 1
    else:
        season_start = now.year - 1
        season_end = now.year
    return f"{season_start}-{str(season_end)[-2:]}"

def main():
    season_str = get_current_season()
    print(f"Fetching all players' stats for season: {season_str}")

    # Create an instance of LeagueDashPlayerStats to get per-game stats for the regular season.
    player_stats = leaguedashplayerstats.LeagueDashPlayerStats(
        season=season_str,
        season_type_all_star="Regular Season",
        per_mode_detailed="PerGame"
    )

    # Retrieve all tables as a list of DataFrames.
    data_frames = player_stats.get_data_frames()

    if not data_frames:
        print("No data frames returned!")
        return

    # Assume the first DataFrame is the main table.
    df = data_frames[0]

    # For debugging, print out the available columns.
    print("Available columns in the data:")
    print(df.columns.tolist())

    # Select the columns relevant for 3PT% and attempts analysis.
    # We include: PLAYER_ID, PLAYER_NAME, TEAM_ABBREVIATION, FG3_PCT (3PT%), and FG3A (3PT attempts)
    selected_columns = ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'FG3_PCT', 'FG3A']
    if not all(col in df.columns for col in selected_columns):
        print("One or more expected columns are missing from the dataframe!")
        print("Available columns:", df.columns.tolist())
        return

    # Filter and optionally sort the DataFrame by FG3_PCT in descending order.
    df_selected = df[selected_columns].sort_values(by='FG3_PCT', ascending=False)

    # Save the result to a CSV file.
    output_csv = "all_players_3pt_percentage_and_attempts.csv"
    df_selected.to_csv(output_csv, index=False)
    print(f"3PT% and attempts data saved to {output_csv}")

if __name__ == '__main__':
    main()
