import requests
import pandas as pd

def process_data(data):
    # Assuming 'resultSets' key in the response; adjust as per actual response structure
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    return df

# The URL from which to fetch data
url = "https://stats.nba.com/stats/leaguedashteamstats"

# Parameters for the request
params = {
    "Conference": "",
    "DateFrom": "",
    "DateTo": "",
    "Division": "",
    "GameScope": "",
    "GameSegment": "",
    "Height": "",
    "ISTRound": "",
    "LastNGames": "10",
    "LeagueID": "00",
    "Location": "",
    "MeasureType": "Advanced",
    "Month": "0",
    "OpponentTeamID": "0",
    "Outcome": "",
    "PORound": "0",
    "PaceAdjust": "N",
    "PerMode": "PerGame",
    "Period": "0",
    "PlayerExperience": "",
    "PlayerPosition": "",
    "PlusMinus": "N",
    "Rank": "N",
    "Season": "2023-24",
    "SeasonSegment": "",
    "SeasonType": "Regular Season",
    "ShotClockRange": "",
    "StarterBench": "",
    "TeamID": "0",
    "TwoWay": "0",
    "VsConference": "",
    "VsDivision": ""
}

# Headers to mimic a request from a web browser
headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Accept-Language": "en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7",
    "Connection": "keep-alive",
    "Host": "stats.nba.com",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Sec-Ch-Ua": "\"Chromium\";v=\"122\", \"Not(A:Brand\";v=\"24\", \"Google Chrome\";v=\"122\"",
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "\"macOS\"",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site"
}

# Making the GET request
response = requests.get(url, headers=headers, params=params)

# Checking if the request was successful
if response.status_code == 200:
    data = response.json()
    print("Data fetched successfully!")
    # You can process the 'data' as needed
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

df_processed = process_data(data)
df_processed['Record'] = df_processed['W'].astype(str) + '-' + df_processed['L'].astype(str)
df_processed.rename(columns={'OFF_RATING': 'OFF RTG'}, inplace=True)
df_processed.rename(columns={'DEF_RATING': 'DEF RTG'}, inplace=True)
df_processed.rename(columns={'AST_TO': 'AST/TO'}, inplace=True)
df_processed.rename(columns={'REB_PCT': 'REB%'}, inplace=True)




Player1 = 'Minnesota Timberwolves'
Player2 = 'Denver Nuggets'
# Simulated user selections
players_selected = [Player1, Player2]
stats_selected = ['Record', 'OFF RTG', 'DEF RTG', 'AST/TO', 'REB%']

player_data = df_processed[df_processed['TEAM_NAME'] == 'Minnesota Timberwolves'].iloc[0]

def format_data_for_player(player, stats):
    player_data = df_processed[df_processed['TEAM_NAME'] == player].iloc[0]
    formatted_data = [player]  # Start with the player's name
    for stat in stats:
        formatted_data.extend([stat, player_data[stat]])
    return formatted_data

# Extract and format data based on user selections
formatted_data = []
for player in players_selected:
    player_formatted_data = format_data_for_player(player, stats_selected)
    formatted_data.extend(player_formatted_data)

# Prepare the header and write to file
header = []
for i in range(1, 5):
    header.extend([f'Player{i}', f'Field{i}', f'Value{i}', f'Field{i} 2', f'Value{i} 2', f'Field{i} 3', f'Value{i} 3', f'Field{i} 4', f'Value{i} 4', f'Field{i} 5', f'Value{i} 5'])

output_filename = '/Users/tonysantoorjian/Documents/advanced_team_stats.csv'
with open(output_filename, 'w') as file:
    file.write(','.join(header) + '\n')
    file.write(','.join(str(item) for item in formatted_data))