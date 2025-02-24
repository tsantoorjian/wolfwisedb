from nba_api.stats.endpoints import (
    PlayerGameLog,
    PlayerCareerStats,
    CommonTeamRoster,
    LeagueDashPlayerStats
)
from nba_api.stats.static import players, teams
import pandas as pd
import time
from requests.exceptions import RequestException
from http.client import RemoteDisconnected
import random

class TimberwolvesRecords:
    def __init__(self):
        # Get Timberwolves team ID
        self.team_id = [team for team in teams.get_teams() if team['full_name'] == 'Minnesota Timberwolves'][0]['id']
        
        # Common statistical categories
        self.stat_categories = [
            'PTS', 'AST', 'REB', 'STL', 'BLK', 
            'FGM', 'FGA', 'FG3M', 'FG3A', 'FTM', 'FTA',
            'OREB', 'DREB', 'TOV', 'PF'
        ]
        
        # Maximum number of retries for API calls
        self.max_retries = 3
    
    def get_top_10_players(self):
        """Get top 10 Timberwolves players by minutes played"""
        # Get player stats for current season
        player_stats = self.api_call_with_retry(
            lambda: LeagueDashPlayerStats(
                team_id_nullable=self.team_id,
                season='2024-25',
                per_mode_detailed='Totals'
            ).get_data_frames()[0]
        )
        
        # Sort by minutes and get top 10
        top_players = player_stats.nlargest(10, 'MIN')
        print("\nTop 10 players by minutes played:")
        for _, player in top_players.iterrows():
            print(f"{player['PLAYER_NAME']}: {player['MIN']} minutes")
            
        return top_players['PLAYER_NAME'].tolist()
    
    def api_call_with_retry(self, api_func, *args, **kwargs):
        """Make API call with retry logic"""
        for attempt in range(self.max_retries):
            try:
                # Add randomized delay between attempts
                if attempt > 0:
                    delay = random.uniform(2, 5)
                    print(f"Retry attempt {attempt + 1}, waiting {delay:.1f} seconds...")
                    time.sleep(delay)
                
                return api_func(*args, **kwargs)
                
            except (RequestException, RemoteDisconnected) as e:
                if attempt == self.max_retries - 1:  # Last attempt
                    raise  # Re-raise the last exception
                print(f"API call failed: {str(e)}, retrying...")
                continue
    
    def get_all_player_records(self):
        """Get records for top 10 Timberwolves players across all stats"""
        all_records = []
        roster = self.get_top_10_players()  # Changed to get top 10 players
        
        for player_name in roster:
            print(f"\nProcessing {player_name}...")
            for stat in self.stat_categories:
                print(f"Getting {stat} records...")
                try:
                    records_df = self.get_player_records(player_name, stat)
                    all_records.append(records_df)
                    # Randomized delay between 1 and 3 seconds
                    delay = random.uniform(1, 3)
                    time.sleep(delay)
                except Exception as e:
                    print(f"Error processing {player_name} for {stat}: {str(e)}")
                    continue
        
        # Combine all records
        if all_records:
            combined_records = pd.concat(all_records, ignore_index=True)
            # Save to CSV
            combined_records.to_csv('timberwolves_player_records.csv', index=False)
            print("\nSaved all records to timberwolves_player_records.csv")
            return combined_records
        return None
    
    def get_player_records(self, player_name, stat='PTS'):
        """Get personal records comparison for a specific player"""
        # Get player ID
        player_info = players.find_players_by_full_name(player_name)[0]
        player_id = player_info['id']
        
        # Get current stats and records with retry logic
        current_stats = self._get_player_current_stats(player_id, stat)
        personal_records = self._get_personal_records(player_id, stat)
        
        # Create records dataframe
        records = []
        for time_interval in ['game', 'season', 'all_time']:
            records.append({
                'time_interval': time_interval,
                'player_comparison_level': 'personal',
                'id': player_id,
                'name': player_name,
                'stat': stat.lower(),
                'current': current_stats[time_interval],
                'record': personal_records[time_interval]
            })
        
        return pd.DataFrame(records)
    
    def _get_player_current_stats(self, player_id, stat):
        """Get current statistics for a player"""
        # Get most recent game stat with retry
        game_log = self.api_call_with_retry(
            lambda: PlayerGameLog(player_id=player_id).get_data_frames()[0]
        )
        current_game = game_log.iloc[0][stat] if not game_log.empty else 0
        
        # Get current season and career totals with retry
        career_stats = self.api_call_with_retry(
            lambda: PlayerCareerStats(player_id=player_id).get_data_frames()[0]
        )
        current_season = career_stats.iloc[-1][stat] if not career_stats.empty else 0
        career_total = career_stats[stat].sum() if not career_stats.empty else 0
        
        return {
            'game': current_game,
            'season': current_season,
            'all_time': career_total
        }
    
    def _get_personal_records(self, player_id, stat):
        """Get personal records for a player"""
        # Game high with retry
        game_log = self.api_call_with_retry(
            lambda: PlayerGameLog(player_id=player_id).get_data_frames()[0]
        )
        game_high = game_log[stat].max() if not game_log.empty else 0
        
        # Season high and career total with retry
        career_stats = self.api_call_with_retry(
            lambda: PlayerCareerStats(player_id=player_id).get_data_frames()[0]
        )
        season_high = career_stats[stat].max() if not career_stats.empty else 0
        career_total = career_stats[stat].sum() if not career_stats.empty else 0
        
        return {
            'game': game_high,
            'season': season_high,
            'all_time': career_total
        }

# Example usage
def main():
    wolves_records = TimberwolvesRecords()
    all_records = wolves_records.get_all_player_records()
    if all_records is not None:
        print("\nFirst few rows of the data:")
        print(all_records.head())

if __name__ == "__main__":
    main()