import requests
import pandas as pd

def process_data(data):
    # Assuming 'resultSets' key in the response; adjust as per actual response structure
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    return df

# The URL from which to fetch data
url = "https://stats.nba.com/stats/teamgamelogs"

# Parameters for the request
params = {
    "DateFrom": "",
    "DateTo": "",
    "GameSegment": "",
    "ISTRound": "",
    "LastNGames": "0",
    "LeagueID": "00",
    "Location": "",
    "MeasureType": "Misc",
    "Month": "0",
    "OpponentTeamID": "0",
    "Outcome": "",
    "PORound": "0",
    "PaceAdjust": "N",
    "PerMode": "Totals",
    "Period": "0",
    "PlayerExperience": "",
    "PlayerPosition": "",
    "PlusMinus": "N",
    "Rank": "N",
    "Season": "2023-24",
    "SeasonSegment": "",
    "SeasonType": "Regular Season",
    "ShotClockRange": "",
    "VsConference": "",
    "VsDivision": "",
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

# Pivot the DataFrame
pivot_df = df_processed.pivot(index='GAME_ID', columns='TEAM_ABBREVIATION')

# Optional: Flatten the MultiIndex columns if needed
pivot_df.columns = ['{}_{}'.format(stat, team) for stat, team in pivot_df.columns]
