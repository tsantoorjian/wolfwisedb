from nba_api.stats.endpoints import playercareerstats, commonteamroster
from nba_api.stats.static import teams
import pandas as pd
import time
import os
import requests

def get_wolves_roster():
    # Get Timberwolves team ID
    wolves = [team for team in teams.get_teams() if team['full_name'] == 'Minnesota Timberwolves'][0]
    wolves_id = wolves['id']
    
    # Get current roster
    roster = commonteamroster.CommonTeamRoster(team_id=wolves_id)
    players = roster.get_data_frames()[0]
    
    return players[['PLAYER_ID', 'PLAYER']]

def get_advanced_stats(player_name):
    """Get advanced stats from the GraphQL API"""
    url = "https://www.nbaapi.com/graphql/"
    
    # GraphQL query for player advanced stats
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
            json={
                'query': query,
                'variables': {}
            },
            headers={'Content-Type': 'application/json'}
        )
        response.raise_for_status()
        data = response.json()
        return data.get('data', {}).get('playerAdvanced', [])
    except Exception as e:
        print(f"Error getting advanced stats for {player_name}: {e}")
        return None

def get_player_career_stats(player_id, player_name):
    try:
        # Get career stats
        career = playercareerstats.PlayerCareerStats(player_id=player_id)
        
        # Regular season stats
        regular_season = career.get_data_frames()[0]
        
        if len(regular_season) == 0:
            return None
            
        # Add season number column (1 for rookie season, incrementing up)
        regular_season['SEASON_NUMBER'] = range(1, len(regular_season) + 1)
        
        # Add player identification columns
        regular_season['PLAYER_ID'] = player_id
        regular_season['PLAYER_NAME'] = player_name
        
        # Select relevant columns and calculate per game stats
        stats_cols = [
            'PLAYER_ID', 'PLAYER_NAME', 'SEASON_NUMBER', 'SEASON_ID', 
            'TEAM_ABBREVIATION', 'PLAYER_AGE', 'GP', 'GS', 'MIN', 'FGM', 
            'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 
            'FT_PCT', 'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 
            'PF', 'PTS'
        ]
        
        df = regular_season[stats_cols].copy()
        
        # Calculate per game stats
        per_game_cols = ['MIN', 'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA', 
                        'OREB', 'DREB', 'REB', 'AST', 'STL', 'BLK', 'TOV', 
                        'PF', 'PTS']
        
        for col in per_game_cols:
            df[f'{col}_PER_GAME'] = df[col] / df['GP']
            
        # Get advanced stats
        advanced_stats = get_advanced_stats(player_name)
        if advanced_stats:
            # Convert advanced stats to DataFrame
            advanced_stats_list = []
            
            # Sort advanced stats by age to ensure correct ordering
            advanced_stats_sorted = sorted(advanced_stats, key=lambda x: x['age'])
            
            # Match stats based on order (youngest to oldest)
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
                # Create DataFrame from advanced stats and merge on SEASON_NUMBER
                advanced_df = pd.DataFrame(advanced_stats_list)
                df = pd.merge(df, advanced_df, on='SEASON_NUMBER', how='left')
            
        return df
        
    except Exception as e:
        print(f"Error getting stats for {player_name}: {e}")
        return None

def get_wolves_year_by_year_stats():
    # Create output directory if it doesn't exist
    output_dir = 'career_stats'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Get current Wolves roster
    roster = get_wolves_roster()
    
    # List to store all player stats
    all_player_stats = []
    
    # Get career stats for each player
    for _, player in roster.iterrows():
        player_id = player['PLAYER_ID']
        player_name = player['PLAYER']
        
        print(f"Getting stats for {player_name}...")
        
        stats = get_player_career_stats(player_id, player_name)
        
        if stats is not None:
            all_player_stats.append(stats)
        
        # Sleep to avoid hitting API rate limits
        time.sleep(1)
    
    if all_player_stats:
        # Combine all player stats into one DataFrame
        combined_stats = pd.concat(all_player_stats, ignore_index=True)
        
        # Save combined stats to CSV
        filename = os.path.join(output_dir, 'wolves_all_players_career_stats.csv')
        combined_stats.to_csv(filename, index=False)
        print(f"Saved combined stats to {filename}")

if __name__ == "__main__":
    get_wolves_year_by_year_stats()
