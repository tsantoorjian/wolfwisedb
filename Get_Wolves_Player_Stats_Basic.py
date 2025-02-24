#!/usr/bin/env python
import datetime
import pandas as pd
import time
from nba_api.stats.endpoints import teamplayerdashboard


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


def get_player_stats_df(dashboard):
    """
    Loops over the DataFrames returned by the endpoint and returns the one that
    contains the PLAYER_ID column.

    If none is found, prints out the columns of each table for debugging.
    """
    data_frames = dashboard.get_data_frames()
    for df in data_frames:
        if "PLAYER_ID" in df.columns:
            return df
    # Debug: print out the columns of each returned table
    print("Debug: No table with 'PLAYER_ID' found. Returned tables:")
    for i, df in enumerate(data_frames):
        print(f"Table {i} columns: {df.columns.tolist()}")
    return None


def fetch_player_stats(team_id, season, last_n_games=None):
    """
    Fetches per-game player stats for a given team and season using the TeamPlayerDashboard endpoint.

    For full season data, the last_n_games parameter is omitted.
    For partial data (e.g., last 5 or 10 games), last_n_games is provided.

    :param team_id: int, NBA team ID.
    :param season: str, season string (e.g., "2024-25").
    :param last_n_games: int or None. If provided, limits the stats to the last n games.
    :return: DataFrame containing player stats.
    """
    # Build the base parameters.
    params = {
        "team_id": team_id,
        "season": season,
        "season_type_all_star": "Regular Season",
        "per_mode_detailed": "PerGame"
    }
    # Only add last_n_games if a value is provided.
    if last_n_games is not None:
        params["last_n_games"] = last_n_games

    try:
        dashboard = teamplayerdashboard.TeamPlayerDashboard(**params)
        df = get_player_stats_df(dashboard)
        if df is None:
            print(f"Warning: No player stats data returned for last_n_games={last_n_games}")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching player stats with last_n_games={last_n_games}: {e}")
        return pd.DataFrame()
    return df


def rename_columns_for_suffix(df, suffix, join_columns=["PLAYER_ID"]):
    """
    Renames all columns in the DataFrame (except for join_columns) by appending a suffix.

    :param df: DataFrame to rename.
    :param suffix: str, the suffix to append (e.g., "last5" or "last10").
    :param join_columns: list of columns that should not be renamed (typically the join key).
    :return: DataFrame with renamed columns.
    """
    df_renamed = df.copy()
    for col in df.columns:
        if col not in join_columns:
            df_renamed.rename(columns={col: f"{col}_{suffix}"}, inplace=True)
    return df_renamed


def main():
    team_id = 1610612750  # Minnesota Timberwolves team ID
    season = get_current_season()
    print(f"Fetching Timberwolves player stats for season: {season}")

    # --- Full Season Stats (without last_n_games) ---
    df_full = fetch_player_stats(team_id, season, last_n_games=None)
    if df_full.empty:
        print("No full season data found.")
        return

    # --- Last 5 Games Stats ---
    df_last5 = fetch_player_stats(team_id, season, last_n_games=5)
    # --- Last 10 Games Stats ---
    df_last10 = fetch_player_stats(team_id, season, last_n_games=10)

    # Rename the columns in the last 5 and last 10 dataframes (to avoid column name collisions)
    df_last5_renamed = rename_columns_for_suffix(df_last5, "last5")
    df_last10_renamed = rename_columns_for_suffix(df_last10, "last10")

    # Merge the DataFrames on PLAYER_ID so that the full season stats are augmented with last 5 and last 10 games stats.
    df_merged = df_full.merge(df_last5_renamed, on="PLAYER_ID", how="left") \
        .merge(df_last10_renamed, on="PLAYER_ID", how="left")

    # Save the merged DataFrame to a CSV file.
    output_csv = "timberwolves_player_stats_with_last_games.csv"
    df_merged.to_csv(output_csv, index=False)
    print(f"Player stats with last 5 and 10 games columns saved to {output_csv}")


if __name__ == '__main__':
    main()
