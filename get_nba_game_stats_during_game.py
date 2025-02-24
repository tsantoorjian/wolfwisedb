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
gamefinder = leaguegamefinder.LeagueGameFinder(season_nullable='2024-25',
                                              league_id_nullable='00',
                                              season_type_nullable='Regular Season')
games = gamefinder.get_data_frames()[0]
# Get a list of distinct game ids
game_ids = games['GAME_ID'].unique().tolist()

#filter to TEAM_ABBREVIATION == 'MIN' and get the game ids
games = games[games['TEAM_ABBREVIATION'] == 'MIN']
game_ids = games['GAME_ID'].unique().tolist()
# Get the latest game from the games DataFrame
latest_game = games.iloc[0]




#grab the most recent game
game_id = game_ids[0]


def fetch_nba_data(url, headers):
    response = requests.get(url, headers=headers)
    data = response.json()
    return data

def process_data(data):
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    return df

# Common headers and URL
url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
    "If-Modified-Since": "Tue, 05 Mar 2024 05:22:46 GMT",
    "If-None-Match": "\"b1614aabd398e87ca9e79f1f64eef0f7\"",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"macOS\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
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

data = fetch_nba_data(url, headers)
# Extract the Los Angeles Lakers players' stats
data_for_df = []
# Determine if Minnesota is the home or away team

# Check if MIN is home or away
if "vs" in latest_game['MATCHUP']:
    players_data = data["game"]["homeTeam"]["players"]
else:
    players_data = data["game"]["awayTeam"]["players"]


for player in players_data:
    player_info = {
        'First Name': player['firstName'],
        'Last Name': player['familyName'],
        'Assists': player['statistics']['assists'],
        'Points': player['statistics']['points'],
        'Rebounds': player['statistics']['reboundsTotal'],
        'Steals': player['statistics']['steals'],
        'Turnovers': player['statistics']['turnovers'],
        'Blocks': player['statistics']['blocks'],
        'Field Goals Made': player['statistics']['fieldGoalsMade'],
        'Field Goals Attempted': player['statistics']['fieldGoalsAttempted'],
        'Three Pointers Made': player['statistics']['threePointersMade'],
        'Three Pointers Attempted': player['statistics']['threePointersAttempted'],
        'plusMinusPoints': player['statistics']['plusMinusPoints'],
    }

    # Append to the data list
    data_for_df.append(player_info)

# Create DataFrame
df = pd.DataFrame(data_for_df)


df['FGs'] = df['Field Goals Made'].astype(str) + '-' + df['Field Goals Attempted'].astype(str)
df['3pt'] = df['Three Pointers Made'].astype(str) + '-' + df['Three Pointers Attempted'].astype(str)
df.rename(columns={'Points': 'PTS'}, inplace=True)
df.rename(columns={'Rebounds': 'REB'}, inplace=True)
df.rename(columns={'Assists': 'AST'}, inplace=True)
df.rename(columns={'Steals': 'STL'}, inplace=True)
df.rename(columns={'Turnovers': 'TOV'}, inplace=True)
df.rename(columns={'Blocks': 'BLK'}, inplace=True)
df['Player'] = df['First Name'] + ' ' + df['Last Name']
#df.to_excel('/Users/tonysantoorjian/Documents/post_game_stats.xlsx')

Player1 = 'Anthony Edwards'
Player2 = 'Julius Randle'
Player3 = 'Rudy Gobert'
Player4 = 'Mike Conley'
Player5 = 'Jaden McDaniels'
# Simulated user selections
players_selected = [Player1, Player2, Player3, Player4, Player5]
stats_selected = {
    Player1: ['PTS', 'REB', 'AST', 'plusMinusPoints'],
    Player2: ['PTS', 'AST', 'STL', 'plusMinusPoints'],
    Player3: ['PTS', 'REB', 'FGs', 'plusMinusPoints'],
    Player4: ['PTS', 'AST', '3pt', 'plusMinusPoints'],
    Player5: ['PTS', 'REB', 'FGs', 'plusMinusPoints']
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
for i in range(1, 6):
    header.extend([f'Player{i}', f'Field{i}', f'Value{i}', f'Field{i} 2', f'Value{i} 2', f'Field{i} 3', f'Value{i} 3', f'Field{i} 4', f'Value{i} 4'])

output_filename = '/Users/tonysantoorjian/Documents/selected_player_stats.csv'
with open(output_filename, 'w') as file:
    file.write(','.join(header) + '\n')
    file.write(','.join(str(item) for item in formatted_data))