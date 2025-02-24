#!/usr/bin/env python
import datetime
import pandas as pd
import time
import re
from nba_api.stats.endpoints import leaguedashlineups


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


def fetch_lineup_data_paginated(lineup_size, season_str):
    all_data = []
    teams = [1610612750]  # You should include all 30 teams

    for team_id in teams:
        print(f"Fetching {lineup_size}-man lineup data for team {team_id}")
        lineup = leaguedashlineups.LeagueDashLineups(
            group_quantity=lineup_size,
            season=season_str,
            season_type_all_star='Regular Season',
            #measure_type_detailed_defense='Advanced',
            team_id_nullable=team_id
        )

        df = lineup.get_data_frames()[0]
        if not df.empty:
            df['LINEUP_SIZE'] = lineup_size  # Add the column here
            all_data.append(df)

        time.sleep(1)  # Avoid rate-limiting

    return pd.concat(all_data, ignore_index=True) if all_data else None



def split_players(group_name):
    """
    Splits a string containing player names using " - " as the delimiter.
    This ensures that names like "Nickeil Alexander-Walker" remain intact.

    :param group_name: str, the original string from the group_name column.
    :return: list of player names.
    """
    return re.split(r'\s+-\s+', group_name)


def main():
    season_str = get_current_season()
    lineup_sizes = [2, 3, 5]
    all_lineups = []

    # Fetch data for each lineup size and save individual CSV files.
    for size in lineup_sizes:
        try:
            df = fetch_lineup_data_paginated(size, season_str)
            if df is None:
                continue

            # Optional: print out the columns for debugging.
            print(f"Columns for {size}-man lineup: {df.columns.tolist()}")

            csv_file = f"lineup_data_{size}man_{season_str}.csv"
            df.to_csv(csv_file, index=False)
            print(f"Saved {size}-man lineup data to {csv_file}")
            all_lineups.append(df)

            time.sleep(1)  # Pause briefly between requests.
        except Exception as e:
            print(f"Error fetching {size}-man lineup data: {e}")

    # Combine all the lineup data into a single DataFrame.
    if all_lineups:
        combined_df = pd.concat(all_lineups, ignore_index=True)
        combined_csv = f"combined_basic_lineup_data_{season_str}.csv"

        # Check for the group_name column (if it's uppercase, rename it for consistency)
        if 'group_name' not in combined_df.columns and 'GROUP_NAME' in combined_df.columns:
            combined_df.rename(columns={'GROUP_NAME': 'group_name'}, inplace=True)

        if 'group_name' in combined_df.columns:
            # Create a temporary column with the split list of player names.
            combined_df['players_list'] = combined_df['group_name'].apply(split_players)

            # Assume a maximum of 5 players per group; create new columns player1 to player5.
            for i in range(5):
                combined_df[f'player{i + 1}'] = combined_df['players_list'].apply(
                    lambda players: players[i] if len(players) > i else None
                )
            # Optionally, remove the temporary players_list column.
            combined_df.drop(columns=['players_list'], inplace=True)
        else:
            print("Column 'group_name' not found in the combined dataframe. Skipping player splitting.")

        combined_df.to_csv(combined_csv, index=False)
        print(f"Combined lineup data with split player columns saved to {combined_csv}")


if __name__ == '__main__':
    main()
