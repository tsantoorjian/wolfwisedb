from nba_api.stats.endpoints import playercareerstats
from nba_api.stats.static import players
import pandas as pd
import time
import os
import requests
from hall_of_fame_list import fetch_nba_hall_of_fame_players
import signal
from contextlib import contextmanager
import threading

@contextmanager
def timeout(seconds):
    """Context manager for timing out operations"""
    timer = threading.Timer(seconds, lambda: (_ for _ in ()).throw(TimeoutError()))
    timer.start()
    try:
        yield
    finally:
        timer.cancel()

def find_player_id(player_name):
    """Find NBA player ID by name"""
    try:
        with timeout(5):  # Timeout after 5 seconds
            # Clean the player name - remove special characters and normalize
            cleaned_name = ''.join(c for c in player_name if c.isalnum() or c.isspace())
            
            nba_players = players.get_players()
            # Try exact match first
            player_matches = [p for p in nba_players if p['full_name'].lower() == player_name.lower()]
            
            # If no exact match, try with cleaned names
            if not player_matches:
                player_matches = [
                    p for p in nba_players 
                    if ''.join(c for c in p['full_name'] if c.isalnum() or c.isspace()).lower() == cleaned_name.lower()
                ]
            
            # If still no match, try partial match
            if not player_matches:
                player_matches = [
                    p for p in nba_players 
                    if cleaned_name.lower() in ''.join(c for c in p['full_name'] if c.isalnum() or c.isspace()).lower()
                ]
                
            return player_matches[0]['id'] if player_matches else None
            
    except TimeoutError:
        print(f"Timeout while processing {player_name} - skipping")
        return None
    except Exception as e:
        print(f"Error finding player ID for {player_name}: {e}")
        return None

def get_advanced_stats(player_name):
    """Get advanced stats from the GraphQL API"""
    url = "https://www.nbaapi.com/graphql/"
    
    query = """
    query PlayerAdvanced {
        playerAdvanced(name: "%s") {
            id
            playerName
            position
            age
            games
            minutesPlayed
            per
            tsPercent
            threePAr
            ftr
            offensiveRbPercent
            defensiveRbPercent
            totalRbPercent
            assistPercent
            stealPercent
            blockPercent
            turnoverPercent
            usagePercent
            offensiveWs
            defensiveWs
            winShares
            winSharesPer
            offensiveBox
            defensiveBox
            box
            vorp
            team
            season
            playerId
        }
    }
    """ % player_name

    try:
        response = requests.post(
            url,
            json={'query': query, 'variables': {}},
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', {}).get('playerAdvanced', [])
    except Exception as e:
        print(f"Error getting advanced stats for {player_name}: {e}")
        return None

def get_player_career_stats(player_id, player_name):
    print(f"Starting to fetch career stats for {player_name}...")
    try:
        print(f"Making API call for {player_name}...")
        career = playercareerstats.PlayerCareerStats(player_id=player_id)
        print(f"Got API response for {player_name}")
        
        regular_season = career.get_data_frames()[0]
        print(f"Converted to DataFrame for {player_name}")
        
        if len(regular_season) == 0:
            print(f"No regular season data found for {player_name}")
            return None
            
        regular_season['SEASON_NUMBER'] = range(1, len(regular_season) + 1)
        regular_season['PLAYER_ID'] = player_id
        regular_season['PLAYER_NAME'] = player_name
        
        stats_cols = [
            'PLAYER_ID', 'PLAYER_NAME', 'SEASON_NUMBER', 'SEASON_ID',
            'TEAM_ABBREVIATION', 'PLAYER_AGE', 'GP', 'GS', 'MIN',
            'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT',
            'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
            'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'
        ]
        
        df = regular_season[stats_cols].copy()
        
        per_game_cols = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA',
                        'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS']
        
        for col in per_game_cols:
            df[f'{col}_PER_GAME'] = df[col] / df['GP']
            
        advanced_stats = get_advanced_stats(player_name)
        if advanced_stats:
            advanced_stats_list = []
            advanced_stats_sorted = sorted(advanced_stats, key=lambda x: x['age'])
            
            for idx, stat in enumerate(advanced_stats_sorted, 1):
                advanced_stats_list.append({
                    'SEASON_NUMBER': idx,
                    'PER': stat['per'],
                    'TS_PCT': stat['tsPercent'],
                    'THREE_PAR': stat['threePAr'],
                    'FTR': stat['ftr'],
                    'OREB_PCT': stat['offensiveRbPercent'],
                    'DREB_PCT': stat['defensiveRbPercent'],
                    'REB_PCT': stat['totalRbPercent'],
                    'AST_PCT': stat['assistPercent'],
                    'STL_PCT': stat['stealPercent'],
                    'BLK_PCT': stat['blockPercent'],
                    'TOV_PCT': stat['turnoverPercent'],
                    'USG_PCT': stat['usagePercent'],
                    'OWS': stat['offensiveWs'],
                    'DWS': stat['defensiveWs'],
                    'WS': stat['winShares'],
                    'WS_PER_48': stat['winSharesPer'],
                    'OBPM': stat['offensiveBox'],
                    'DBPM': stat['defensiveBox'],
                    'BPM': stat['box'],
                    'VORP': stat['vorp']
                })
            
            if advanced_stats_list:
                advanced_df = pd.DataFrame(advanced_stats_list)
                df = pd.merge(df, advanced_df, on='SEASON_NUMBER', how='left')
        
        return df
        
    except Exception as e:
        print(f"Error getting stats for {player_name}: {e}")
        return None

def clean_player_name(name):
    """Clean up player names that might have encoding issues"""
    # Remove duplicate chunks that might appear due to encoding issues
    if 'â' in name:
        # Split on â and remove duplicates while preserving order
        parts = name.split('â')
        seen = set()
        cleaned_parts = []
        for part in parts:
            if part and part not in seen:
                seen.add(part)
                cleaned_parts.append(part)
        name = ' '.join(cleaned_parts)
    
    # Remove any remaining special characters
    name = ''.join(c for c in name if c.isalnum() or c.isspace())
    
    # Clean up extra spaces
    name = ' '.join(name.split())
    
    return name

def get_hof_year_by_year_stats():
    # Create output directory if it doesn't exist
    output_dir = 'career_stats'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get Hall of Fame players
    try:
        hof_players = fetch_nba_hall_of_fame_players()
        
        # Clean up player names
        for player in hof_players:
            player['Name'] = clean_player_name(player['Name'])
            
    except Exception as e:
        print(f"Error fetching HOF players: {e}")
        return
    
    # List to store all player stats
    all_player_stats = []
    failed_players = []
    
    # Get career stats for each HOF player
    total_players = len(hof_players)
    for i, player in enumerate(hof_players, 1):
        try:
            player_name = player['Name']
            print(f"\nStarting to process {i}/{total_players}: {player_name}")
            
            print(f"Looking up player ID for {player_name}")
            player_id = find_player_id(player_name)
            
            if player_id:
                print(f"Found ID {player_id} for {player_name}")
                
                try:
                    with timeout(30):  # Add timeout for the entire stats gathering process
                        stats = get_player_career_stats(player_id, player_name)
                except TimeoutError:
                    print(f"Timeout while getting stats for {player_name}")
                    failed_players.append(f"{player_name} - Timeout while getting stats")
                    continue
                
                if stats is not None:
                    stats['HOF_INDUCTION_YEAR'] = player['Year']
                    all_player_stats.append(stats)
                    print(f"Successfully processed {player_name}")
                else:
                    failed_players.append(f"{player_name} - No stats found")
            else:
                failed_players.append(f"{player_name} - No player ID found")
                print(f"Could not find player ID for {player_name}")
            
            print(f"Sleeping for rate limit after {player_name}")
            time.sleep(1)
            print(f"Finished processing {player_name}")
            
        except Exception as e:
            print(f"Error processing {player_name}: {e}")
            failed_players.append(f"{player_name} - Error: {str(e)}")
            continue
    
    if all_player_stats:
        try:
            # Combine all player stats into one DataFrame
            combined_stats = pd.concat(all_player_stats, ignore_index=True)
            
            # Save combined stats to CSV
            filename = os.path.join(output_dir, 'hof_players_career_stats.csv')
            combined_stats.to_csv(filename, index=False)
            print(f"\nSaved combined stats to {filename}")
            
            # Save list of failed players
            if failed_players:
                failed_filename = os.path.join(output_dir, 'failed_players.txt')
                with open(failed_filename, 'w') as f:
                    f.write('\n'.join(failed_players))
                print(f"Saved list of {len(failed_players)} failed players to {failed_filename}")
                
            print(f"\nProcessed {len(all_player_stats)} players successfully")
            print(f"Failed to process {len(failed_players)} players")
            
        except Exception as e:
            print(f"Error saving results: {e}")
    else:
        print("No player stats were collected")

if __name__ == "__main__":
    get_hof_year_by_year_stats()
