import requests
from bs4 import BeautifulSoup
import pandas as pd
import uuid
import os
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Supabase client
supabase_url = 'https://kuthirbcjtofsdwsfhkj.supabase.co'
supabase_key = os.getenv('SUPABASE_KEY')
supabase = create_client(supabase_url, supabase_key)

def scrape_team_leaders():
    # URL of the team leaders page
    url = "https://www.basketball-reference.com/teams/MIN/leaders_season.html"
    
    # Send request with headers to avoid blocking
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        # Fetch the page
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        response.encoding = 'utf-8'
        
        # Parse HTML
        soup = BeautifulSoup(response.text, 'html.parser', from_encoding='utf-8')
        
        # Find all stat boxes
        stat_boxes = soup.find_all('div', class_='data_grid_box')
        
        all_data = []
        
        # Process each box
        for box in stat_boxes:
            # Get category from caption
            caption = box.find('caption')
            if not caption:
                continue
            category = caption.text.strip()
            
            # Find the table
            table = box.find('table', class_='columns')
            if not table:
                continue
                
            # Extract rows
            rows = table.find_all('tr')
            for row in rows:
                # Get rank
                rank_cell = row.find('td', class_='rank')
                if not rank_cell:
                    continue
                rank = rank_cell.text.strip().rstrip('.')
                
                # Get player and year
                who_cell = row.find('td', class_='who')
                if not who_cell:
                    continue
                    
                player = who_cell.find('a').text.strip()
                year = who_cell.find('span', class_='desc').text.strip()
                
                # Get value and convert to numeric
                value_cell = row.find('td', class_='value')
                if not value_cell:
                    continue
                value = value_cell.text.strip()
                
                # Clean and convert value to numeric
                try:
                    # Remove commas and whitespace
                    cleaned_value = value.replace(',', '').strip()
                    # Convert to float first
                    numeric_value = float(cleaned_value)
                    
                    # Convert to integer by multiplying by 100 to preserve 2 decimal places
                    integer_value = int(numeric_value * 100)
                    
                    # Convert back to float with proper decimal places
                    final_value = integer_value / 100.0
                    
                    # Ensure value is within safe range for JSON
                    if abs(final_value) > 1e9:  # Reduced maximum size
                        print(f"Skipping large value: {final_value} for {category}")
                        continue
                        
                    all_data.append([category, rank, player, year, final_value])
                except ValueError as e:
                    print(f"Skipping invalid value: {value} - Error: {str(e)}")
                    continue
        
        # Create DataFrame
        df = pd.DataFrame(all_data, columns=['Category', 'Rank', 'Player', 'Year', 'Value'])
        
        # Additional safety check for numeric values
        df['Value'] = pd.to_numeric(df['Value'], errors='coerce')
        df = df.dropna(subset=['Value'])  # Remove any rows where Value is NaN
        df = df[df['Value'].abs() < 1e9]  # Filter out extremely large values
        
        # Convert Rank to integer
        df['Rank'] = pd.to_numeric(df['Rank'], downcast='integer')
        
        # Add UUIDs
        df['id'] = [str(uuid.uuid4()) for _ in range(len(df))]
        
        # Save to Supabase
        save_to_supabase(df)
        
        # Also save to CSV as backup
        df.to_csv('timberwolves_leaders.csv', index=False, encoding='utf-8-sig')
        
        return df
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

def save_to_supabase(df):
    """Save DataFrame to Supabase"""
    try:
        # Make a copy of the DataFrame to avoid modifying the original
        df_to_save = df.copy()
        
        # Drop any rows with NA values
        df_to_save = df_to_save.dropna()
        
        # Convert Rank to integer
        df_to_save['Rank'] = pd.to_numeric(df_to_save['Rank'], errors='coerce').fillna(0).astype(int)
        
        # Convert Value column to string with appropriate decimal places
        def format_value(x):
            if pd.isna(x):  # Handle NA values
                return "0"
            if x < 1:  # For very small numbers
                return f"{x:.3f}"
            elif x < 10:  # For single digit numbers
                return f"{x:.2f}"
            else:  # For larger numbers
                return f"{int(x)}"  # No decimals for large numbers
        
        df_to_save['Value'] = df_to_save['Value'].apply(format_value)
        
        # Print some debug info
        print("\nSample of formatted values:")
        print(df_to_save[['Category', 'Value']].head(10))
        
        # Convert DataFrame to list of dictionaries
        records = df_to_save.to_dict('records')
        
        # Delete existing records
        supabase.table('timberwolves_season_leaders').delete().neq('id', '00000000-0000-0000-0000-000000000000').execute()
        
        # Insert new records
        result = supabase.table('timberwolves_season_leaders').insert(records).execute()
        
        print(f"Successfully saved {len(records)} records to timberwolves_season_leaders table")
        
        # Print preview of the data
        print("\nPreview of inserted data:")
        print(df_to_save[['id', 'Category', 'Rank', 'Player', 'Value']].head())
        print("\n")
        
    except Exception as e:
        print(f"Error saving to Supabase: {str(e)}")
        if 'records' in locals():
            print("Failed records:")
            for record in records[:5]:
                print(record)

if __name__ == "__main__":
    data = scrape_team_leaders()
    if data is not None:
        print("\nFirst few rows of the data:")
        print(data.head())