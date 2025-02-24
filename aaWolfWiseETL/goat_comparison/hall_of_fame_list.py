import requests
from bs4 import BeautifulSoup
import pandas as pd

def fetch_nba_hall_of_fame_players():
    url = "https://www.basketball-reference.com/awards/hof.html"
    
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:110.0) "
            "Gecko/20100101 Firefox/110.0"
        )
    }
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        print(f"Request failed with status code {response.status_code}")
        return []
    
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find("table", {"id": "hof"})
    if not table:
        print("Could not find the Hall of Fame table on the page.")
        return []
    
    players = []
    # Loop over each row in the table body
    for row in table.tbody.find_all("tr", recursive=False):
        # Get the induction year from the <th> cell
        year_th = row.find("th", {"data-stat": "year_id"})
        name_td = row.find("td", {"data-stat": "name_full"})
        category_td = row.find("td", {"data-stat": "category"})
        
        if not (year_th and name_td and category_td):
            continue

        # Only include rows with category "Player"
        if category_td.get_text(strip=True).lower() != "player":
            continue

        # Check the <span> within the name cell to determine the league.
        span = name_td.find("span")
        if span:
            first_link = span.find("a")
            if first_link:
                league_text = first_link.get_text(strip=True).upper()
                # If the first link isn't "PLAYER", skip this row.
                if league_text != "PLAYER":
                    continue

        # Now remove the <span> so that only the actual name remains.
        for sp in name_td.find_all("span"):
            sp.decompose()
        name_text = name_td.get_text(strip=True)
        induction_year = year_th.get_text(strip=True)
        category_text = category_td.get_text(strip=True)
        
        players.append({
            "Year": induction_year,
            "Name": name_text,
            "Category": category_text
        })
    
    return players

if __name__ == "__main__":
    nba_players = fetch_nba_hall_of_fame_players()
    if nba_players:
        df = pd.DataFrame(nba_players)
        csv_filename = "nba_hall_of_fame_players.csv"
        df.to_csv(csv_filename, index=False)
        print(f"Saved {len(df)} rows to {csv_filename}")
    else:
        print("No NBA Hall of Fame player data found.")
