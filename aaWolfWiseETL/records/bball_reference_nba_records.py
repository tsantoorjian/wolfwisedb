import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
from supabase import create_client
from dotenv import load_dotenv
import uuid

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = 'https://kuthirbcjtofsdwsfhkj.supabase.co'
supabase_key = os.getenv('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)

def load_urls():
    """Load URLs from the CSV file"""
    df = pd.read_csv('basketball_reference_links.csv')
    # Print debug info
    print(f"Found {len(df)} URLs in CSV file")
    
    # Don't group by Link Text as multiple entries can have same text (e.g., "Single Season")
    # Instead, return list of tuples with (Link Text, URL)
    return list(zip(df['Link Text'], df['URL']))

def get_stat_type(url):
    """Extract stat type from URL"""
    # Extract everything between 'leaders/' and before '_season', '_career', or '_active'
    stat = url.split('leaders/')[1]
    for suffix in ['_season', '_career', '_active']:
        if suffix in stat:
            stat = stat.split(suffix)[0]
            break
    return stat

def clean_value(value):
    """Clean the value string and check if it's numeric"""
    # Remove any commas and whitespace
    value = value.replace(',', '').strip()
    
    try:
        # Try to convert to float to test if numeric
        float(value)
        return True
    except ValueError:
        return False

def scrape_stat_page(url, record_type):
    """Scrape a single stat page"""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        # Ensure proper encoding of the response
        response.encoding = 'utf-8'
        
        soup = BeautifulSoup(response.text, "html.parser", from_encoding='utf-8')
        data = []
        stat_type = get_stat_type(url)
        
        # Try different table IDs based on the record type
        if record_type in ['Career', 'Active']:
            possible_tables = ['tot', 'nba']
            table = None
            for table_id in possible_tables:
                table = soup.find('table', {'id': table_id})
                if table:
                    break
            if not table:
                table = soup.find('table')
        else:
            table = soup.find("table", id=lambda x: x and x.startswith('stats_'))
            if not table:
                table = soup.find('table', {'class': 'stats_table'})
        
        if not table:
            print(f"No table found for {url}")
            return None

        rows = []
        if table.find('tbody'):
            rows = table.find('tbody').find_all('tr')
        else:
            rows = table.find_all('tr')
        
        if not rows:
            print(f"No rows found in table for {url}")
            return None

        last_rank = None
        
        for row in rows:
            if 'thead' in row.get('class', []) or 'thead2' in row.get('class', []):
                continue
                
            if 'thead' in row.get('class', []) or not row.find_all(['td', 'th']):
                continue

            cells = row.find_all(['td', 'th'])
            if len(cells) >= 3:
                rank_text = cells[0].text.strip().rstrip('.')
                if rank_text:
                    last_rank = rank_text
                rank = last_rank
                
                player = cells[1].text.strip()
                value = cells[2].text.strip()
                
                # Only add row if value is numeric
                if clean_value(value):
                    season = ""
                    if len(cells) > 3 and record_type == "Single Season":
                        season = cells[3].text.strip()
                    
                    data.append([rank, player, value, season, record_type, stat_type])

        if not data:
            print(f"No data extracted from table for {url}")
            return None

        columns = ["Rank", "Player", "Value", "Season", "Record Type", "Stat Type"]
        df = pd.DataFrame(data, columns=columns)
        
        # Convert Value column to numeric, removing any commas
        df['Value'] = df['Value'].str.replace(',', '').astype(float)
        
        return df
        
    except requests.exceptions.RequestException as e:
        print(f"Request error for {url}: {str(e)}")
        return None
    except Exception as e:
        print(f"Error scraping {url}: {str(e)}")
        print(f"Error type: {type(e)}")
        return None

def append_to_csv(df, filename):
    """Append DataFrame to CSV, create file with headers if it doesn't exist"""
    if not os.path.exists(filename):
        df.to_csv(filename, index=False, encoding='utf-8-sig')
    else:
        df.to_csv(filename, mode='a', header=False, index=False, encoding='utf-8-sig')

def save_to_supabase(df):
    """Save DataFrame to Supabase"""
    try:
        # Add UUID column
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Convert DataFrame to list of dictionaries
        records = df.to_dict('records')
        
        # Delete existing records
        supabase.table('nba_records').delete().neq('id', '0').execute()
        
        # Insert new records
        result = supabase.table('nba_records').insert(records).execute()
        
        print(f"Successfully saved {len(records)} records to nba_records table")
        
        # Print preview of the data
        print("\nPreview of inserted data:")
        print(df[['id', 'Rank', 'Player', 'Value', 'Stat Type']].head())
        print("\n")
        
    except Exception as e:
        print(f"Error saving to Supabase: {str(e)}")

def main():
    # Load URLs from CSV
    url_pairs = load_urls()
    
    # Track success and failure counts
    success_count = 0
    failure_count = 0
    
    # Create empty list to store all DataFrames
    all_data = []
    
    # Process each URL
    total_urls = len(url_pairs)
    for i, (record_type, url) in enumerate(url_pairs, 1):
        print(f"\nProcessing {i}/{total_urls}: {record_type} - {url}")
        df = scrape_stat_page(url, record_type)
        if df is not None:
            all_data.append(df)
            print(f"Successfully scraped {len(df)} records")
            success_count += 1
        else:
            print(f"Failed to scrape data from {url}")
            failure_count += 1
        time.sleep(2)  # Increased delay to be more conservative
    
    # Combine all DataFrames
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        
        # Save to Supabase
        save_to_supabase(final_df)
        
        # Also save to CSV as backup
        final_df.to_csv('basketball_reference_records.csv', index=False)
    
    print(f"\nScraping complete:")
    print(f"Successfully scraped: {success_count} pages")
    print(f"Failed to scrape: {failure_count} pages")

if __name__ == "__main__":
    main()
