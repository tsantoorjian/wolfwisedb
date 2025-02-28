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
from nba_api.stats.library.http import NBAStatsHTTP

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

class ProxyNBAStatsHTTP(NBAStatsHTTP):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        proxy_url = os.getenv('PROXY_URL')
        if proxy_url:
            self.proxies = {
                'http': proxy_url,
                'https': proxy_url,
            }
            logger.info(f"Proxy configured: {proxy_url}")
        else:
            self.proxies = {}
            logger.warning("No PROXY_URL found in environment variables; running without proxy.")

    def send_api_request(self, *args, **kwargs):
        return super().send_api_request(*args, proxies=self.proxies, **kwargs)

# Replace the default HTTP client with our proxy-enabled version
leaguedashplayerstats.LeagueDashPlayerStats.nba_stats_http_class = ProxyNBAStatsHTTP

# Modify the NBA API headers globally to avoid request blocking
ProxyNBAStatsHTTP.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nba.com/",
    "Origin": "https://www.nba.com/",
    "Host": "stats.nba.com",
    "Connection": "keep-alive"
})

def api_call_with_retry(api_func, max_retries=3):
    """Make API call with retry logic"""
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                delay = random.uniform(2, 5)
                logger.info(f"Retry attempt {attempt + 1}, waiting {delay:.1f} seconds...")
                time.sleep(delay)

            return api_func()
            
        except (RequestException, RemoteDisconnected) as e:
            if attempt == max_retries - 1:
                logger.error(f"Final attempt failed: {str(e)}")
                raise
            logger.warning(f"API call failed: {str(e)}, retrying...")
            continue

def get_player_stats() -> pd.DataFrame:
    """Fetch player stats from NBA API"""
    logger.info("Initiating NBA stats retrieval...")

    logger.info("Fetching per game stats from NBA API...")
    basic_stats_per_game = api_call_with_retry(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Base',
            season='2024-25'
        ).get_data_frames()[0]
    )

    logger.info("Fetching total minutes from NBA API...")
    basic_stats_totals = api_call_with_retry(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='Totals',
            measure_type_detailed_defense='Base',
            season='2024-25'
        ).get_data_frames()[0]
    )

    basic_stats_totals['MIN'] = basic_stats_totals['MIN'].round().astype(int)

    logger.info("Fetching advanced stats from NBA API...")
    advanced_stats = api_call_with_retry(
        lambda: leaguedashplayerstats.LeagueDashPlayerStats(
            per_mode_detailed='PerGame',
            measure_type_detailed_defense='Advanced',
            season='2024-25'
        ).get_data_frames()[0]
    )

    logger.info(f"Successfully retrieved data for {len(basic_stats_per_game)} players")

    stats_mapping = {
        'FG3_PCT': '3pt percentage',
        'FG_PCT': 'Fg %', 
        'STL': 'Steals per game',
        'AST': 'Assists per game',
        'TOV': 'Turnovers per game',
        'BLK': 'Blocks per game',
        'PTS': 'Points Per Game'
    }

    transformed_data = []
    total_stats = len(stats_mapping) + 1  # +1 for EFG%

    for idx, (original_col, new_name) in enumerate(stats_mapping.items(), 1):
        logger.info(f"Processing stat {idx}/{total_stats}: {new_name} (from {original_col})")

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

        if idx < total_stats:
            delay = random.uniform(0.5, 1)
            logger.info(f"Waiting {delay:.1f} seconds before processing next stat...")
            time.sleep(delay)

    logger.info(f"Processing stat {total_stats}/{total_stats}: EFG %")
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

    logger.info("Data transformation completed")
    return pd.DataFrame(transformed_data)

def load_to_supabase(df: pd.DataFrame) -> None:
    """Load data to Supabase"""
    logger.info("Initializing Supabase connection...")

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

    records = df.to_dict('records')
    logger.info(f"Preparing to load {len(records)} records to Supabase")

    try:
        logger.info("Upserting data into distribution_stats table...")
        supabase.table('distribution_stats').upsert(records, on_conflict='player_id,stat').execute()
        logger.info("Data successfully loaded to Supabase")
    except Exception as e:
        logger.error(f"Error loading data to Supabase: {str(e)}")
        raise

def check_ip_with_proxy():
    """Fetch and log the IP address using the proxy"""
    proxy_url = os.getenv('PROXY_URL')
    proxies = {'http': proxy_url, 'https': proxy_url} if proxy_url else {}
    try:
        response = requests.get('https://api.ipify.org', proxies=proxies, timeout=10)
        ip = response.text
        logger.info(f"External IP address detected: {ip}")
    except Exception as e:
        logger.error(f"Failed to check IP: {str(e)}")

def main():
    logger.info("Starting stat distribution data collection")
    check_ip_with_proxy()  # Add this to log IP before API calls
    try:
        df = get_player_stats()
        load_to_supabase(df)
        logger.info("Script completed successfully")
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main()
