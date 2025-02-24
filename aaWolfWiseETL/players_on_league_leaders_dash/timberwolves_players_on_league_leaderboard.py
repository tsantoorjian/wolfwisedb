import requests
from bs4 import BeautifulSoup
import pandas as pd
import logging
import re
from supabase import create_client
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = 'https://kuthirbcjtofsdwsfhkj.supabase.co'
supabase_key = os.getenv('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)

# URL of the team leaderboard page (2024-25 season)
URL = "https://www.basketball-reference.com/teams/MIN/2025.html"

# Headers to mimic a real browser request
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def clean_stat_name(caption):
    """Extract clean stat name from caption"""
    # First try data-tip
    if 'data-tip' in caption.attrs:
        data_tip = caption['data-tip']
        # Extract text from bold tag if present
        bold_match = re.search(r'<b>(.*?)</b>', data_tip)
        if bold_match:
            return bold_match.group(1)
    
    # Then try to get text from bold tag
    bold_tag = caption.find('b')
    if bold_tag:
        return bold_tag.text.strip()
    
    # Finally, just use caption text
    return caption.text.strip()

def extract_value_and_ranking(cell):
    """Extract value and ranking from a cell"""
    # Get all text content first
    text_content = cell.get_text(strip=True)
    
    # Try to find value and ranking with new pattern
    # Look for number followed by ranking in parentheses
    match = re.search(r'([\d.]+)\s*(?:\(([\d]+(?:st|nd|rd|th))\))?', text_content)
    
    if match:
        value = float(match.group(1))
        
        # Check for ranking in a link first
        ranking_link = cell.find('a', string=re.compile(r'\(\d+(?:st|nd|rd|th)\)'))
        if ranking_link:
            ranking = ranking_link.text.strip('()')
        else:
            # Use the ranking from regex if found
            ranking = match.group(2) if match.group(2) else ''
            
        return value, ranking
    
    return None, None

def save_to_supabase(df):
    """Save DataFrame to Supabase"""
    try:
        # First delete all existing records
        logger.info("Deleting existing records from players_on_league_leaderboard table...")
        supabase.table('players_on_league_leaderboard').delete().gte('Value', 0).execute()
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Insert new records
        logger.info("Inserting new records into players_on_league_leaderboard table...")
        result = supabase.table('players_on_league_leaderboard').insert(records).execute()
        
        logger.info(f"Successfully saved {len(records)} records to players_on_league_leaderboard table")
        
        # Print preview of the data
        logger.info("\nPreview of inserted data:")
        logger.info(df[['Stat Category', 'Player', 'Value', 'Ranking']].head())
        logger.info("\n")
        
    except Exception as e:
        logger.error(f"Error saving to Supabase: {str(e)}")
        # Print the first record to see the data structure
        if records:
            logger.error(f"Sample record: {records[0]}")

try:
    # Request the page content
    logger.info(f"Requesting URL: {URL}")
    response = requests.get(URL, headers=HEADERS)
    response.raise_for_status()
    
    # Extract the commented section
    html_content = response.text
    logger.info("Searching for leaderboard section...")
    
    # Find the commented section containing the leaderboard
    commented_section = re.search(r'<!--\s*?(<div.*?id="div_leaderboard".*?</div>)\s*?-->', html_content, re.DOTALL)
    if not commented_section:
        raise ValueError("Leaderboard section not found in HTML")

    # Parse the leaderboard HTML
    leaderboard_html = commented_section.group(1)
    soup = BeautifulSoup(leaderboard_html, "html.parser")
    
    # Initialize list for storing the extracted data
    data = []

    # Find all tables in the leaderboard section
    tables = soup.find_all("table")
    logger.info(f"Found {len(tables)} stat tables")

    for idx, table in enumerate(tables, 1):
        caption = table.find("caption")
        if caption:
            stat_name = clean_stat_name(caption)
            logger.info(f"Processing category: {stat_name}")

            # Process each row in the table
            rows = table.find_all("tr")
            for row in rows:
                try:
                    # Find the cell containing player info (might be td or single class)
                    cell = row.find('td', class_='single') or row.find('td')
                    if cell and cell.find('a'):  # Make sure we have a player link
                        player_name = cell.find('a').text.strip()
                        value, ranking = extract_value_and_ranking(cell)
                        
                        if value is not None:
                            data.append({
                                "Stat Category": stat_name,
                                "Player": player_name,
                                "Value": value,
                                "Ranking": ranking
                            })
                            logger.info(f"Added {player_name} - {stat_name}: {value} ({ranking})")
                
                except Exception as e:
                    logger.error(f"Error processing row: {str(e)}")
                    logger.error(f"Row HTML: {row.prettify()}")

    # Convert to DataFrame
    df = pd.DataFrame(data)
    logger.info(f"\nCreated DataFrame with {len(df)} rows")

    # Save to Supabase
    save_to_supabase(df)
    
    # Also save to CSV as backup
    df.to_csv("team_leaderboard_2025.csv", index=False)
    logger.info("Data saved to team_leaderboard_2025.csv")

except Exception as e:
    logger.error(f"An error occurred: {str(e)}")
    if 'response' in locals():
        logger.error(f"Response content: {response.text[:1000]}...")