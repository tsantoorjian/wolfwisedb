# Import necessary libraries
import pandas as pd
from nba_api.stats.endpoints import playergamelog
from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import commonteamroster

# Function to get player ID
def get_player_id(player_name):
    player_dict = players.get_players()
    player = [p for p in player_dict if p['full_name'] == player_name]
    if player:
        return player[0]['id']
    else:
        raise ValueError(f"Player '{player_name}' not found.")

# Function to get team ID
def get_team_id(team_name):
    team_dict = teams.get_teams()
    team = [t for t in team_dict if t['full_name'] == team_name]
    if team:
        return team[0]['id']
    else:
        raise ValueError(f"Team '{team_name}' not found.")

# Function to get player's game logs
def get_player_game_logs(player_id, season='ALL'):
    gamelog = playergamelog.PlayerGameLog(player_id=player_id, season=season)
    df = gamelog.get_data_frames()[0]
    return df

# Function to calculate player's statistics
def calculate_player_stats(df):
    # Current Game
    current_game_points = df.iloc[0]['PTS']
    max_game_points = df['PTS'].max()

    # Season Stats
    current_season = df.iloc[0]['SEASON_ID']
    season_df = df[df['SEASON_ID'] == current_season]
    current_season_points = season_df['PTS'].sum()
    max_season_points = df.groupby('SEASON_ID')['PTS'].sum().max()

    # All-Time
    total_points = df['PTS'].sum()

    return {
        'current_game_points': current_game_points,
        'max_game_points': max_game_points,
        'current_season_points': current_season_points,
        'max_season_points': max_season_points,
        'total_points': total_points
    }

# Function to get current team players
def get_current_team_players(team_id):
    roster = commonteamroster.CommonTeamRoster(team_id=team_id)
    df = roster.get_data_frames()[0]
    player_ids = df['PLAYER_ID'].tolist()
    return player_ids

# Function to retrieve team player game logs
def get_team_player_game_logs(player_ids, team_id, season='ALL'):
    all_logs = pd.DataFrame()
    for pid in player_ids:
        try:
            logs = get_player_game_logs(pid, season=season)
            # Filter games where the player was on the team
            team_games = logs[logs['TEAM_ID'] == team_id]
            all_logs = pd.concat([all_logs, team_games], ignore_index=True)
        except Exception as e:
            print(f"Error retrieving logs for player ID {pid}: {e}")
    return all_logs

# Function to calculate team records at player level
def calculate_team_player_records(df):
    # Max points in a game by any player on the team
    max_game_points = df['PTS'].max()

    # Max points in a season by any player on the team
    season_points = df.groupby(['PLAYER_ID', 'SEASON_ID'])['PTS'].sum().reset_index()
    max_season_points = season_points['PTS'].max()

    # All-Time points by a player on the team
    total_points = df.groupby('PLAYER_ID')['PTS'].sum().reset_index()
    max_total_points = total_points['PTS'].max()

    return {
        'max_game_points': max_game_points,
        'max_season_points': max_season_points,
        'max_total_points': max_total_points
    }

# Function to update player, team, and league stats
def update_player_team_league_stats(player_name, team_name):
    # Get player and team IDs
    player_id = get_player_id(player_name)
    team_id = get_team_id(team_name)

    # Get player's game logs and stats
    player_logs = get_player_game_logs(player_id)
    player_stats = calculate_player_stats(player_logs)

    # Get current team players
    player_ids = get_current_team_players(team_id)

    # Get team players' game logs and records
    team_player_logs = get_team_player_game_logs(player_ids, team_id)
    team_player_records = calculate_team_player_records(team_player_logs)

    # League records at player level (placeholders)
    # For comprehensive league records, consider using a dataset from sources like Kaggle
    # Here, we use placeholder values for demonstration
    league_player_records = {
        'max_game_points': 100,       # Wilt Chamberlain's 100-point game
        'max_season_points': 4029,    # Wilt Chamberlain's 1961-62 season
        'max_total_points': 38387     # Kareem Abdul-Jabbar's career points (before LeBron broke it)
    }

    # Combine all stats
    stats = {
        'player': player_stats,
        'team_records': team_player_records,
        'league_records': league_player_records
    }

    return stats

# Example Usage
if __name__ == "__main__":
    # Input player and team information
    player_name = "LeBron James"
    team_name = "Los Angeles Lakers"

    # Update stats
    stats = update_player_team_league_stats(player_name, team_name)
