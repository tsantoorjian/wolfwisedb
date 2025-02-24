import requests
import pandas as pd
import sqlite3

def process_data(data):
    # Assuming 'resultSets' key in the response; adjust as per actual response structure
    headers = data['resultSets'][0]['headers']
    rows = data['resultSets'][0]['rowSet']
    df = pd.DataFrame(rows, columns=headers)
    #calculate poss_pct rank for each playtype
    df['POSS_PCT_RANK'] = df['POSS_PCT'].rank(ascending=False)
    #multiply percentile by 30 and round down to get PPP_rank
    df['PPP_RANK'] = 31 - (df['PERCENTILE'] * 30).apply(int)
    return df

# The URL from which to fetch data
url = "https://stats.nba.com/stats/synergyplaytypes"

# Parameters for the request
params = {
    "LeagueID": "00",
    "PerMode": "PerGame",
    "PlayType": "Isolation",
    "PlayerOrTeam": "T",
    "SeasonType": "Regular Season",
    "SeasonYear": "2023-24",
    "TypeGrouping": "defensive"
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

play_types = ['OffScreen', 'Isolation', 'Transition', 'Postup', 'Spotup',
              'Handoff', 'Cut', 'OffRebound', 'Misc', 'PRRollman', 'PRBallHandler']

# Loop through each play type and fetch the data and append to df
df_list = []
for play_type in play_types:
    params['PlayType'] = play_type
    data = requests.get(url, headers=headers, params=params).json()
    df_list.append(process_data(data))

df = pd.concat(df_list)

df_offensive_list = []
params['TypeGrouping'] = 'offensive'
for play_type in play_types:
    params['PlayType'] = play_type
    data = requests.get(url, headers=headers, params=params).json()
    df_offensive_list.append(process_data(data))

df_offensive = pd.concat(df_offensive_list)

#load to excel on different sheets
with pd.ExcelWriter('/Users/tonysantoorjian/Documents/play_types.xlsx') as writer:
    df.to_excel(writer, sheet_name='defensive')
    df_offensive.to_excel(writer, sheet_name='offensive')
# load to df
df_processed = process_data(data)


conn = sqlite3.connect('/Users/tonysantoorjian/Documents/ww_db.db')

#insert to sqlite3
df.to_sql('team_play_types_defensive', conn, if_exists='replace', index=False)
df_offensive.to_sql('team_play_types_offensive', conn, if_exists='replace', index=False)


# Don't forget to close the connection
conn.close()