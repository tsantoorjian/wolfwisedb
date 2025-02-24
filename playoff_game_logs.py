import pandas as pd
from basketball_reference_scraper.seasons import get_schedule
from datetime import datetime

for year in '2024':
    print(f"Fetching playoff games for {year}...")
    season_schedule = get_schedule(2024, playoffs=True)

def fetch_nba_playoff_games(start_year, end_year):
    all_games = []

    for year in range(start_year, end_year + 1):
        print(f"Fetching playoff games for {year}...")
        season_schedule = get_schedule(year, playoffs=True)

        for _, game in season_schedule.iterrows():
            game_details = {
                'Date': game['DATE'].strftime('%Y-%m-%d') if pd.notna(game['DATE']) else None,
                'Home Team': game['HOME'],
                'Visitor Team': game['VISITOR'],
                'Home Points': game['HOME_PTS'],
                'Visitor Points': game['VISITOR_PTS']
            }
            all_games.append(game_details)

    return pd.DataFrame(all_games)

if __name__ == '__main__':
    start_year = datetime.now().year - 0
    end_year = datetime.now().year
    games_df = fetch_nba_playoff_games(start_year, end_year)
    filename = 'NBA_Playoff_Games_{}-{}.xlsx'.format(start_year, end_year)
    games_df.to_excel(filename, index=False)
    print(f'Excel file has been saved as {filename}')
