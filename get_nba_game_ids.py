import requests
import pandas as pd
import numpy as np
import io
from nba_api.stats.static import teams
from nba_api.stats.endpoints import leaguegamefinder
import requests
import pandas as pd
from openpyxl import load_workbook

# get game logs from the reg season
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2023-24',
                                              league_id_nullable='00',
                                              season_type_nullable='Regular Season')
games = gamefinder.get_data_frames()[0]
# Get a list of distinct game ids
game_ids = games['GAME_ID'].unique().tolist()

#filter to TEAM_ABBREVIATION == 'MIN' and get the game ids
games = games[games['TEAM_ABBREVIATION'] == 'MIN']
game_ids = games['GAME_ID'].unique().tolist()

#grab the most recent game
game_id = game_ids[0]


def fetch_nba_data(url, headers, params):
    response = requests.get(url, headers=headers, params=params)
    data = response.json()
    return data

def process_data(data):
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    return df

# Common headers and URL
url = "https://stats.nba.com/stats/boxscoretraditionalv3"
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not A Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"macOS\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
}

params = {
    "GameID": game_id,
    "LeagueID": "00",
    "endPeriod": "4",
    "endRange": "28800",
    "rangeType": "1",
    "startPeriod": "1",
    "startRange": "0",
}


# Function to create a DataFrame from team data
def create_team_stats_df(team_data):
    # Initialize a list to hold player data
    players_data = []

    # Loop through each player and collect their statistics
    for player in team_data['players']:
        player_stats = player['statistics']
        player_info = {
            "Player": f"{player['firstName']} {player['familyName']}",
            "Position": player['position'],
        }
        # Merge player info with their statistics
        player_info.update(player_stats)

        # Add to the players_data list
        players_data.append(player_info)

    # Convert the list of player data into a DataFrame
    df = pd.DataFrame(players_data)
    return df

data = fetch_nba_data(url, headers, params)
# Check both home and away team for 'Minnesota'
home_team_data = data['boxScoreTraditional']['homeTeam']
away_team_data = data['boxScoreTraditional']['awayTeam']

if home_team_data['teamCity'] == 'Minnesota':
    df = create_team_stats_df(home_team_data)
elif away_team_data['teamCity'] == 'Minnesota':
    df = create_team_stats_df(away_team_data)
else:
    print("Minnesota team not found in this game.")
    df = pd.DataFrame()  # Empty DataFrame if team not found


df['FGs'] = df['fieldGoalsMade'].astype(str) + '-' + df['fieldGoalsAttempted'].astype(str)
df['3pts'] = df['threePointersMade'].astype(str) + '-' + df['threePointersAttempted'].astype(str)
df.rename(columns={'points': 'PTS'}, inplace=True)
df.rename(columns={'reboundsTotal': 'REB'}, inplace=True)
df.rename(columns={'assists': 'AST'}, inplace=True)
df.rename(columns={'steals': 'STL'}, inplace=True)
df.rename(columns={'turnovers': 'TOV'}, inplace=True)
df.rename(columns={'blocks': 'BLK'}, inplace=True)
#df.to_excel('/Users/tonysantoorjian/Documents/post_game_stats.xlsx')

Player1 = 'Mike Conley'
Player2 = 'Rudy Gobert'
Player3 = 'Naz Reid'
# Simulated user selections
players_selected = [Player1, Player2, Player3]
stats_selected = {
    Player1: ['PTS', 'AST', 'STL', 'plusMinusPoints'],
    Player2: ['PTS', 'REB', 'BLK', 'plusMinusPoints'],
    Player3: ['PTS', '3pts', 'REB', 'plusMinusPoints']
}




# Function to format data for a single player
def format_data_for_player(player, stats):
    player_data = df[df['Player'] == player].iloc[0]
    formatted_data = [player]  # Start with the player's name
    for stat in stats:
        formatted_data.extend([stat, player_data[stat]])
    return formatted_data

# Extract and format data based on user selections
formatted_data = []
for player in players_selected:
    player_formatted_data = format_data_for_player(player, stats_selected[player])
    formatted_data.extend(player_formatted_data)

# Prepare the header and write to file
header = []
for i in range(1, 4):
    header.extend([f'Player{i}', f'Field{i}', f'Value{i}', f'Field{i} 2', f'Value{i} 2', f'Field{i} 3', f'Value{i} 3', f'Field{i} 4', f'Value{i} 4'])

output_filename = '/Users/tonysantoorjian/Documents/selected_player_stats.csv'
with open(output_filename, 'w') as file:
    file.write(','.join(header) + '\n')
    file.write(','.join(str(item) for item in formatted_data))