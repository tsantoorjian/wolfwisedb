import requests
from bs4 import BeautifulSoup
import pandas as pd

# URL of the Basketball Reference Leaders page
URL = "https://www.basketball-reference.com/leaders/"

# Request the page
response = requests.get(URL, headers={"User-Agent": "Mozilla/5.0"})

if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")

    # Find all links
    links = soup.find_all("a", href=True)

    # Extract link text and URLs
    data = []
    base_url = "https://www.basketball-reference.com"

    for link in links:
        text = link.text.strip()
        href = link["href"]

        # Ensure the link is valid
        if href.startswith("/leaders/"):
            full_url = base_url + href
            data.append([text, full_url])

    # Convert to DataFrame
    df = pd.DataFrame(data, columns=["Link Text", "URL"])

    # Save to CSV
    df.to_csv("basketball_reference_links.csv", index=False)

    print("Scraping completed. Links saved to basketball_reference_links.csv")
else:
    print(f"Failed to retrieve page, status code: {response.status_code}")
