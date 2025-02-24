import pandas as pd
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder


def fetch_playoff_scores(season_id):
    # Get all games for the season
    gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable=season_id, league_id_nullable='00')
    games = gamefinder.get_data_frames()[0]

    # Filter for playoff games
    playoff_games = games[games['SEASON_ID'].str.contains('4')]

    # Collecting game details
    game_details = []
    for index, game in playoff_games.iterrows():
        game_detail = {
            'Game_ID': game['GAME_ID'],
            'Matchup': game['MATCHUP'],
            'Game_Date': game['GAME_DATE'],
            'Team_ID': game['TEAM_ID'],
            'Team_Abbreviation': game['TEAM_ABBREVIATION'],
            'PTS': game['PTS']
        }
        game_details.append(game_detail)

    return pd.DataFrame(game_details)


# Example usage:
seasons = ['2021-22', '2022-23']  # Adjust these as per the seasons of interest
all_games_details = pd.concat([fetch_playoff_scores(season) for season in seasons])
print(all_games_details)

# Function to transform DataFrame
def transform_dataframe(games_df):
    # Initialize a list to hold the transformed data
    transformed_data = []

    # Group by Game_ID to handle each game
    for game_id, group in games_df.groupby('Game_ID'):
        game_info = {
            'Game_ID': game_id,
            'Game_Date': group['Game_Date'].iloc[0],
            f'{group["Team_Abbreviation"].iloc[0]}_PTS': group['PTS'].iloc[0],
            f'{group["Team_Abbreviation"].iloc[1]}_PTS': group['PTS'].iloc[1],
            'Matchup': f'{group["Team_Abbreviation"].iloc[0]} vs. {group["Team_Abbreviation"].iloc[1]}'
        }
        transformed_data.append(game_info)

    # Convert list of dictionaries to DataFrame
    reshaped_df = pd.DataFrame(transformed_data)
    return reshaped_df

# Apply the transformation
transformed_df = transform_dataframe(all_games_details)
print(transformed_df)