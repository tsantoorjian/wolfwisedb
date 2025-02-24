import requests
import pandas as pd
import numpy as np
import os
import sqlite3
import streamlit as st
import pyodbc
from datetime import datetime


# Function to get the latest game ID for the Minnesota Timberwolves
def get_latest_game_id():
    url = 'https://data.nba.com/data/10s/v2015/json/mobile_teams/nba/2024/league/00_full_schedule.json'
    response = requests.get(url)

    if response.status_code == 200:
        schedule_data = response.json()
        games = schedule_data.get('lscd', [])

        # List to hold Timberwolves' games
        timberwolves_games = []

        # Iterate through each month's schedule
        for month in games:
            for game in month.get('mscd', {}).get('g', []):
                home_team = game.get('h', {}).get('ta')
                away_team = game.get('v', {}).get('ta')
                game_date = game.get('gdte')

                # Check if Timberwolves are playing
                if home_team == 'MIN' or away_team == 'MIN':
                    timberwolves_games.append({
                        'game_id': game.get('gid'),
                        'date': game_date,
                        'home_team': home_team,
                        'away_team': away_team
                    })

        # Sort games by date in descending order to get the most recent game
        timberwolves_games.sort(key=lambda x: datetime.strptime(x['date'], '%Y-%m-%d'), reverse=True)

        # Get the current date
        current_date = datetime.now()

        # Iterate through the sorted games and find the latest game on or before the current date
        for game in timberwolves_games:
            game_date = datetime.strptime(game['date'], '%Y-%m-%d')
            # Only consider games on or before the current date
            if game_date <= current_date:
                return game['game_id']

        # If no past games are found, print a message
        print("No past games found for the Timberwolves.")
    else:
        print("Failed to retrieve the schedule data.")

    return None


# Get the latest game ID for the Timberwolves
game_id = get_latest_game_id()
if not game_id:
    print("Error retrieving game ID")
else:
    # Define headers and URL for the specific game stats
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    headers = {"User-Agent": "Mozilla/5.0"}

    # Fetch NBA data for the latest game
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()

        # Determine if MIN is home or away
        is_home_team = data["game"]["homeTeam"]["teamTricode"] == "MIN"
        players_data = data["game"]["homeTeam"]["players"] if is_home_team else data["game"]["awayTeam"]["players"]

        # Extract relevant player stats
        data_for_df = [
            {
                'Player': f"{player['firstName']} {player['familyName']}",
                'PTS': player['statistics']['points'],
                'REB': player['statistics']['reboundsTotal'],
                'AST': player['statistics']['assists'],
                'STL': player['statistics']['steals'],
                'TOV': player['statistics']['turnovers'],
                'BLK': player['statistics']['blocks'],
                'FGs': f"{player['statistics']['fieldGoalsMade']}-{player['statistics']['fieldGoalsAttempted']}",
                '3pt': f"{player['statistics']['threePointersMade']}-{player['statistics']['threePointersAttempted']}",
                'plusMinusPoints': player['statistics']['plusMinusPoints']
            }
            for player in players_data
        ]

        # Convert DataFrame to ensure all values are strings
        df = pd.DataFrame(data_for_df).astype(str)

        # Database connection setup
        uid = os.getenv("DB_UID")
        pwd = os.getenv("DB_PWD")

        connection_string = (
            "Driver={ODBC Driver 18 for SQL Server};"
            "Server=tcp:wolfwise.database.windows.net,1433;"
            "Database=WolfwiseDB;"
            f"Uid={uid};"
            f"Pwd={pwd};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            "Connection Timeout=60;"
        )

        # Insert data into SQL database
        try:
            connection = pyodbc.connect(connection_string)
            cursor = connection.cursor()

            # Check if the table exists
            cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'player_stats'")
            table_exists = cursor.fetchone()[0] > 0

            # Create table if it does not exist
            if not table_exists:
                cursor.execute('''
                    CREATE TABLE player_stats (
                        Player NVARCHAR(100),
                        PTS NVARCHAR(10),
                        REB NVARCHAR(10),
                        AST NVARCHAR(10),
                        BLK NVARCHAR(10),
                        STL NVARCHAR(10),
                        TOV NVARCHAR(10),
                        FGs NVARCHAR(10),
                        threePt NVARCHAR(10),
                        plusMinusPoints NVARCHAR(10)
                    )
                ''')

            # Clear existing data
            cursor.execute('DELETE FROM player_stats')

            # Insert new data
            for _, row in df.iterrows():
                cursor.execute('''
                    INSERT INTO player_stats (Player, PTS, REB, AST, BLK, STL, TOV, FGs, threePt, plusMinusPoints)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', tuple(row))

            # Commit changes
            connection.commit()
            cursor.close()
            connection.close()
        except pyodbc.Error as e:
            print("Error while connecting to SQL Server:", e)
    else:
        print("Failed to fetch game data.")
