#!/usr/bin/env python3
import pandas as pd
from nba_api.stats.endpoints import LeagueDashPlayerStats, CommonTeamRoster
import time
import logging
from supabase import create_client
from dotenv import load_dotenv
import os

# Configure logging to output progress messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Minnesota Timberwolves team ID and season definition
TEAM_ID = 1610612750
SEASON = "2024-25"
# Base URL for player images (as per your sample)
BASE_IMAGE_URL = "https://kuthirbcjtofsdwsfhkj.supabase.co/storage/v1/object/public/player-images/"

# Load environment variables and initialize Supabase client
load_dotenv()
supabase_url = 'https://kuthirbcjtofsdwsfhkj.supabase.co'
supabase_key = os.getenv('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)

def fetch_timberwolves_stats(season=SEASON, team_id=TEAM_ID):
    """
    Fetches season stats for Minnesota Timberwolves players, merges in roster details,
    adds a nickname, constructs an image URL, and reorders the DataFrame columns to match
    the desired order and naming conventions.
    """
    logging.info("Starting to fetch Timberwolves stats for season %s", season)
    time.sleep(1)
    
    # Fetch stats using LeagueDashPlayerStats
    logging.info("Fetching player stats from LeagueDashPlayerStats")
    stats = LeagueDashPlayerStats(season=season, team_id_nullable=team_id)
    df_stats = stats.get_data_frames()[0]
    logging.info("Stats data fetched with %d records", len(df_stats))
    
    # Fetch roster details using CommonTeamRoster (for position and jersey number)
    logging.info("Fetching roster data from CommonTeamRoster")
    roster = CommonTeamRoster(team_id=team_id)
    df_roster = roster.get_data_frames()[0]
    logging.info("Roster data fetched with %d records", len(df_roster))
    
    # Rename stats DataFrame columns to match our naming conventions.
    # Note: 'MIN' represents minutes per game.
    rename_stats = {
        'PLAYER_ID': 'player_id',
        'PLAYER_NAME': 'player_name',
        'GP': 'games_played',
        'W': 'wins',
        'L': 'losses',
        'W_PCT': 'win_percentage',
        'MIN': 'minutes_per_game',
        'FGM': 'field_goals_made',
        'FGA': 'field_goals_attempted',
        'FG_PCT': 'field_goal_percentage',
        'FG3M': 'three_pointers_made',
        'FG3A': 'three_pointers_attempted',
        'FG3_PCT': 'three_point_percentage',
        'FTM': 'free_throws_made',
        'FTA': 'free_throws_attempted',
        'FT_PCT': 'free_throw_percentage',
        'OREB': 'offensive_rebounds',
        'DREB': 'defensive_rebounds',
        'REB': 'total_rebounds',
        'AST': 'assists',
        'TOV': 'turnovers',
        'STL': 'steals',
        'BLK': 'blocks',
        'BLKA': 'blocked_attempts',
        'PF': 'personal_fouls',
        'PFD': 'personal_fouls_drawn',
        'PTS': 'points',
        'PLUS_MINUS': 'plus_minus',
        'NBA_FANTASY_PTS': 'nba_fantasy_pts',
        'DD2': 'double_doubles',
        'TD3': 'triple_doubles'
    }
    logging.info("Renaming stats DataFrame columns")
    df_stats = df_stats.rename(columns=rename_stats)
    
    # Rename roster DataFrame columns to include position and jersey number
    rename_roster = {
        'PLAYER_ID': 'player_id',
        'POSITION': 'position',
        'NUM': 'jersey_number'
    }
    logging.info("Renaming roster DataFrame columns for position and jersey number")
    df_roster = df_roster.rename(columns=rename_roster)
    df_roster = df_roster[['player_id', 'position', 'jersey_number']]
    
    # Merge stats and roster DataFrames on player_id.
    logging.info("Merging stats data with roster data")
    df_merged = pd.merge(df_stats, df_roster, on='player_id', how='left')
    logging.info("Merging complete. Total records: %d", len(df_merged))
    
    # Add a nickname column by taking the first word of the player's name.
    logging.info("Adding nickname column")
    df_merged['nickname'] = df_merged['player_name'].apply(lambda x: x.split()[0] if isinstance(x, str) else None)
    
    # Construct the image_url column by converting the player's name to a lower-case, underscore-separated string.
    logging.info("Adding image_url column")
    df_merged['image_url'] = df_merged['player_name'].apply(
        lambda x: BASE_IMAGE_URL + x.lower().replace(" ", "_") + ".png" if isinstance(x, str) else None
    )
    
    # Create an 'id' column as a sequential unique identifier starting at 1.
    logging.info("Adding id column")
    df_merged.insert(0, 'id', range(1, len(df_merged) + 1))
    
    # Reorder columns to match the desired order
    desired_order = [
        'id',
        'player_id',
        'player_name',
        'nickname',
        'games_played',
        'wins',
        'losses',
        'win_percentage',
        'minutes_per_game',
        'field_goals_made',
        'field_goals_attempted',
        'field_goal_percentage',
        'three_pointers_made',
        'three_pointers_attempted',
        'three_point_percentage',
        'free_throws_made',
        'free_throws_attempted',
        'free_throw_percentage',
        'offensive_rebounds',
        'defensive_rebounds',
        'total_rebounds',
        'assists',
        'turnovers',
        'steals',
        'blocks',
        'blocked_attempts',
        'personal_fouls',
        'personal_fouls_drawn',
        'points',
        'plus_minus',
        'nba_fantasy_pts',
        'double_doubles',
        'triple_doubles',
        'image_url',
        'position',
        'jersey_number'
    ]
    logging.info("Reordering columns to match specified order")
    df_merged = df_merged[desired_order]
    
    logging.info("Data processing complete")
    return df_merged

if __name__ == "__main__":
    logging.info("Script started")
    
    # Fetch, process, and merge the Timberwolves stats and roster data
    df_stats = fetch_timberwolves_stats()
    
    try:
        # Replace inf and -inf with None, and convert NaN to None
        df_stats = df_stats.replace([float('inf'), float('-inf')], None)
        df_stats = df_stats.where(pd.notnull(df_stats), None)
        
        # Convert percentage columns to proper scale (0-1 instead of 0-100)
        percentage_columns = [
            'win_percentage', 'field_goal_percentage', 
            'three_point_percentage', 'free_throw_percentage'
        ]
        for col in percentage_columns:
            df_stats[col] = df_stats[col].apply(lambda x: x/100 if x is not None else None)
        
        # Convert DataFrame to list of dictionaries for Supabase
        records = df_stats.to_dict('records')
        
        # Delete existing records
        supabase.table('nba_player_stats').delete().neq('id', 0).execute()
        
        # Insert new records
        result = supabase.table('nba_player_stats').insert(records).execute()
        
        logging.info("Successfully saved stats to Supabase table 'nba_player_stats'")
        
    except Exception as e:
        logging.error("Error saving stats to Supabase: %s", str(e))
    
    logging.info("Script finished")
