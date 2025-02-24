import requests
import pandas as pd
import sqlite3
import time


def fetch_and_process_data(url, headers):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        # Access the JSON structure here to fetch result sets as required
        headers = data['pageProps']['data']['resultSets'][0]['headers']
        rows = data['pageProps']['data']['resultSets'][0]['rowSet']
        df = pd.DataFrame(rows, columns=headers)
        return df
    except Exception as e:
        print(f"Failed to retrieve data from {url}: {e}")
        return pd.DataFrame()


# Base URL and headers for requests
base_url = "https://www.nba.com/_next/data/5TJ1HMfFntBMF9Rwhi_OT/en/stats"
headers = {
    "Accept": "*/*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "www.nba.com",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
}

# Define the tracking types and entities to iterate over
tracking_types = ['Drives', 'Defense', 'CatchShoot', 'Passing', 'Possessions', 'PullUpShot', 'Rebounding', 'Efficiency',
                  'SpeedDistance', 'ElbowTouch', 'PostTouch', 'PaintTouch']
entities = ['teams', 'players']  # 'teams' for team data, 'players' for player data

# Connect to SQLite database
conn = sqlite3.connect('/Users/tonysantoorjian/Documents/ww_db.db')

# Iterate over each combination of tracking type and entity
for tracking_type in tracking_types:
    for entity in entities:
        # Build the dynamic URL based on tracking type and entity
        url = f"{base_url}/{entity}/{tracking_type.lower()}.json"
        print(f"Requesting data for {tracking_type} - {entity}")

        # Fetch and process data
        df = fetch_and_process_data(url, headers)

        # If data is available, save to the database with a unique table name
        if not df.empty:
            table_name = f"{entity}_{tracking_type}"
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Data saved to table {table_name}")
        else:
            print(f"No data available for {tracking_type} - {entity}")

        # Delay to avoid request rate limits
        time.sleep(1)

# Close the database connection
conn.close()
