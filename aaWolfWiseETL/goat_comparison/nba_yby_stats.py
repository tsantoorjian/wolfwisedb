from nba_api.stats.endpoints import playercareerstats, commonallplayers
from nba_api.stats.static import teams
import pandas as pd
import time
import os
import requests
from wolves_year_by_year_stats import get_wolves_roster, get_advanced_stats, get_player_career_stats

def get_all_active_players():
    """Get all active NBA players"""
    # Get list of all active players
    all_players = commonallplayers.CommonAllPlayers(is_only_current_season=1)
    players_df = all_players.get_data_frames()[0]
    
    # Filter for active players - players who are still playing
    current_season = "2023"  # NBA seasons are labeled like "2023-24"
    active_players = players_df[players_df['TO_YEAR'].str.startswith(current_season, na=False)]
    
    if len(active_players) == 0:
        # Fallback: if no players found, try getting players who played in the most recent season
        most_recent_season = players_df['TO_YEAR'].max()
        active_players = players_df[players_df['TO_YEAR'] == most_recent_season]
    
    return active_players[['PERSON_ID', 'DISPLAY_FIRST_LAST']]

def get_non_wolves_players():
    """Get all active NBA players except Timberwolves players"""
    # Get all active players
    all_players = get_all_active_players()
    
    # Get current Wolves roster
    wolves_roster = get_wolves_roster()
    
    # Filter out Wolves players
    non_wolves = all_players[~all_players['PERSON_ID'].isin(wolves_roster['PLAYER_ID'])]
    
    # Rename columns to match the format used in get_player_career_stats
    non_wolves = non_wolves.rename(columns={
        'PERSON_ID': 'PLAYER_ID',
        'DISPLAY_FIRST_LAST': 'PLAYER'
    })
    
    return non_wolves

def get_nba_year_by_year_stats():
    """Get career stats for all non-Wolves NBA players"""
    # Create output directory if it doesn't exist
    output_dir = 'career_stats'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get non-Wolves players
    players = get_non_wolves_players()
    print(f"Found {len(players)} non-Wolves players to process")
    
    # List to store all player stats
    all_player_stats = []
    failed_players = []
    
    # Get career stats for each player
    total_players = len(players)
    for idx, (_, player) in enumerate(players.iterrows(), 1):
        player_id = player['PLAYER_ID']
        player_name = player['PLAYER']
        
        print(f"Processing {idx}/{total_players}: {player_name}")
        
        stats = get_player_career_stats(player_id, player_name)
        
        if stats is not None:
            all_player_stats.append(stats)
        else:
            failed_players.append(player_name)
        
        # Sleep to avoid hitting API rate limits
        time.sleep(1)
    
    if all_player_stats:
        # Combine all player stats into one DataFrame
        combined_stats = pd.concat(all_player_stats, ignore_index=True)
        
        # Save combined stats to CSV
        filename = os.path.join(output_dir, 'nba_all_players_career_stats.csv')
        combined_stats.to_csv(filename, index=False)
        print(f"\nSaved stats for {len(all_player_stats)} players to {filename}")
    
    # Save failed players to a text file
    if failed_players:
        failed_filename = os.path.join(output_dir, 'failed_nba_players.txt')
        with open(failed_filename, 'w') as f:
            f.write('\n'.join(failed_players))
        print(f"Failed to get stats for {len(failed_players)} players. See {failed_filename}")

if __name__ == "__main__":
    get_nba_year_by_year_stats()
