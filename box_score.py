import pandas as pd
from nba_api.stats.endpoints import TeamInfoCommon
from nba_api.stats.static import teams
from nba_api.stats.endpoints import TeamGameLogs


# Get the list of NBA teams
nba_teams = teams.get_teams()

# Create a dictionary of team IDs
team_ids_dict = {team['full_name']: team['id'] for team in nba_teams}

# Create an empty DataFrame to store the data
df = pd.DataFrame()

# Loop through the teams and append to the df
for team_name, team_id in team_ids_dict.items():
    team_info = TeamInfoCommon(team_id=team_id)
    df_team = team_info.get_data_frames()[0]
    df_team['TeamName'] = team_name  # Adding the team name to the DataFrame
    df_team['Season'] = '2023-24'  # Adding the season to the DataFrame
    df = pd.concat([df, df_team], ignore_index=True)

gamedatapull = TeamGameLogs(
    league_id_nullable='00',  # nba 00, g_league 20, wnba 10
    team_id_nullable='1610612750',  # can specify a specific team_id
    season_nullable='2023-24',
    season_type_nullable='Playoffs'  # Regular Season, Playoffs, Pre Season
)

df_season = gamedatapull.get_data_frames()[0]