from nba_api.stats.endpoints import leaguedashplayerstats
import pandas as pd
from supabase import create_client
import os
from typing import List, Dict
import time
import random
from requests.exceptions import RequestException
from http.client import RemoteDisconnected
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def api_call_with_retry(api_func, max_retries=3):
    """Make API call with retry logic"""
    for attempt in range(max_retries):
        try:
            # Add randomized delay between attempts
            if attempt > 0:
                delay = random.uniform(2, 5)
                logger.info(f"Retry attempt {attempt + 1}, waiting {delay:.1f} seconds...")
                time.sleep(delay)
            
            return api_func()
            
        except (RequestException, RemoteDisconnected) as e:
            if attempt == max_retries - 1:  # Last attempt
                logger.error(f"Final attempt failed: {str(e)}")
                raise  # Re-raise the last exception
            logger.warning(f"API call failed: {str(e)}, retrying...")
            continue

def get_player_stats() -> pd.DataFrame:
    """Fetch player stats from NBA API"""
    logger.info("Initiating NBA stats retrieval...")
    
    # Get basic player stats (per game)
    logger.info("Fetching per game stats from NBA API...")
    basic_stats_per_game = api_call_with_retry(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Base',
            season='2024-25'
        ).get_data_frames()[0]
    )
    
    # Get basic player stats (totals for minutes)
    logger.info("Fetching total minutes from NBA API...")
    basic_stats_totals = api_call_with_retry(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='Totals',
            measure_type_detailed_defense='Base',
            season='2024-25'
        ).get_data_frames()[0]
    )
    
    # Convert minutes to integers
    basic_stats_totals['MIN'] = basic_stats_totals['MIN'].round().astype(int)
    
    # Get advanced stats for EFG%
    logger.info("Fetching advanced stats from NBA API...")
    advanced_stats = api_call_with_retry(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Advanced',
            season='2024-25'
        ).get_data_frames()[0]
    )
    
    logger.info(f"Successfully retrieved data for {len(basic_stats_per_game)} players")

    # Select and rename relevant columns
    stats_mapping = {
        'FG3_PCT': '3pt percentage',
        'FG_PCT': 'Fg %', 
        'STL': 'Steals per game',
        'AST': 'Assists per game',
        'TOV': 'Turnovers per game',
        'BLK': 'Blocks per game',
        'PTS': 'Points Per Game'
    }

    # Create list to store transformed data
    transformed_data = []
    total_stats = len(stats_mapping) + 1  # +1 for EFG%

    # Transform basic stats
    for idx, (original_col, new_name) in enumerate(stats_mapping.items(), 1):
        logger.info(f"Processing stat {idx}/{total_stats}: {new_name} (from {original_col})")
        
        # Merge per game stats with total minutes
        stat_data = basic_stats_per_game[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', original_col]].merge(
            basic_stats_totals[['PLAYER_ID', 'MIN']],
            on='PLAYER_ID',
            how='left'
        )
        
        stat_records = stat_data.rename(columns={
            'PLAYER_ID': 'player_id',
            'PLAYER_NAME': 'player_name',
            'TEAM_ABBREVIATION': 'team_abbreviation',
            'MIN': 'minutes_played',
            original_col: 'value'
        })
        stat_records['stat'] = new_name
        transformed_data.extend(stat_records.to_dict('records'))
        
        # Log some sample values for verification
        sample_players = stat_data.nlargest(3, original_col)
        logger.info(f"Top 3 players for {new_name}:")
        for _, player in sample_players.iterrows():
            logger.info(f"  {player['PLAYER_NAME']}: {player[original_col]:.3f} (Total MIN: {player['MIN']})")
        
        # Add small delay between processing different stats
        if idx < total_stats:
            delay = random.uniform(0.5, 1)
            logger.info(f"Waiting {delay:.1f} seconds before processing next stat...")
            time.sleep(delay)

    # Add EFG% from advanced stats
    logger.info(f"Processing stat {total_stats}/{total_stats}: EFG %")
    
    # Merge with total minutes
    efg_data = advanced_stats[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ABBREVIATION', 'EFG_PCT']].merge(
        basic_stats_totals[['PLAYER_ID', 'MIN']],
        on='PLAYER_ID',
        how='left'
    )
    
    efg_records = efg_data.rename(columns={
        'PLAYER_ID': 'player_id',
        'PLAYER_NAME': 'player_name',
        'TEAM_ABBREVIATION': 'team_abbreviation',
        'MIN': 'minutes_played',
        'EFG_PCT': 'value'
    })
    efg_records['stat'] = 'EFG %'
    transformed_data.extend(efg_records.to_dict('records'))
    
    # Log EFG% sample values
    sample_efg = efg_data.nlargest(3, 'EFG_PCT')
    logger.info(f"Top 3 players for EFG %:")
    for _, player in sample_efg.iterrows():
        logger.info(f"  {player['PLAYER_NAME']}: {player['EFG_PCT']:.3f} (Total MIN: {player['MIN']})")

    logger.info("Data transformation completed")
    return pd.DataFrame(transformed_data)

def load_to_supabase(df: pd.DataFrame) -> None:
    """Load data to Supabase"""
    logger.info("Initializing Supabase connection...")
    
    # Initialize Supabase client
    supabase_url = os.environ.get('SUPABASE_URL')
    supabase_key = os.environ.get('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        error_msg = "Missing required environment variables:"
        if not supabase_url:
            error_msg += " SUPABASE_URL"
        if not supabase_key:
            error_msg += " SUPABASE_KEY"
        logger.error(error_msg)
        raise ValueError(error_msg)

    try:
        supabase = create_client(supabase_url, supabase_key)
    except Exception as e:
        logger.error(f"Failed to create Supabase client: {str(e)}")
        raise

    # Convert DataFrame to list of dictionaries
    records = df.to_dict('records')
    logger.info(f"Preparing to load {len(records)} records to Supabase")

    try:
        # Upsert data (update if exists, insert if doesn't)
        logger.info("Upserting data into distribution_stats table...")
        supabase.table('distribution_stats').upsert(
            records,
            on_conflict='player_id,stat'  # Specify the composite primary key
        ).execute()
        logger.info("Data successfully loaded to Supabase")
        
    except Exception as e:
        logger.error(f"Error loading data to Supabase: {str(e)}")
        raise

def main():
    logger.info("Starting stat distribution data collection")
    try:
        # Get player stats
        df = get_player_stats()
        
        # Load to Supabase
        load_to_supabase(df)
        
        # Print summary
        logger.info("\nData collection summary:")
        for stat in df['stat'].unique():
            stat_count = len(df[df['stat'] == stat])
            logger.info(f"  {stat}: {stat_count} records")
        
        logger.info("Script completed successfully")
        
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
