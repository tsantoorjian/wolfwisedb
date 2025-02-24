import pandas as pd
from basketball_reference_scraper.players import get_game_logs

# Step 1: Read Player Names from CSV
# Assuming the CSV has a column "PlayerName" with player names
#player_df = pd.read_csv('/Users/tonysantoorjian/Documents/players_input.csv')

import pandas as pd
from basketball_reference_scraper.players import get_game_logs

# Ask the user whether to use the most current week or not
use_current_week = input("Do you want to use the most current week for comparison? (yes/no): ").strip().lower()

# Assuming the CSV has a column "PlayerName" with player names
player_df = pd.read_csv('/Users/tonysantoorjian/Documents/players_input.csv')
player_names = player_df['PlayerName'].tolist()

all_players_game_logs = []  # List to hold game logs dataframes

for player_name in player_names:
    try:
        df = get_game_logs(player_name, 2024, playoffs=False)
        df.insert(0, 'Player', player_name)

        # Ensure 'Date' is in datetime format for extraction
        df['Date'] = pd.to_datetime(df['DATE'])

        # Add 'Year-Week' column, making sure the week starts on a Monday
        df['Year-Week'] = df['Date'].dt.strftime('%Y-W%W')

        # Append the dataframe to the list
        all_players_game_logs.append(df)
        print(f"Retrieved game logs for {player_name}")
    except Exception as e:
        print(f"Could not retrieve game logs for {player_name}: {e}")

if all_players_game_logs:
    combined_df = pd.concat(all_players_game_logs, ignore_index=True)

    # Convert numeric columns, excluding 'Player' and 'Date'
    cols = combined_df.columns.drop(['Player', 'Date'])
    combined_df[cols] = combined_df[cols].apply(pd.to_numeric, errors='ignore')

    # Prepare to write to Excel
    with pd.ExcelWriter('/Users/tonysantoorjian/Documents/game_logs_and_stats.xlsx') as writer:  # Update the path as necessary
        # Detailed game logs
        combined_df.to_excel(writer, sheet_name='Game Logs', index=False)

        # Calculate weekly 3P% and FG%
        combined_df['3P%'] = combined_df['3P'] / combined_df['3PA']
        combined_df['FG%'] = combined_df['FG'] / combined_df['FGA']

        # Replace NaN values with 0 for percentages
        combined_df['3P%'].fillna(0, inplace=True)
        combined_df['FG%'].fillna(0, inplace=True)

        # Aggregate data
        stats_to_sum = ['3PA', 'FGA', '3P', 'FG']
        aggregated_data = combined_df.groupby(['Player', 'Year-Week'])[stats_to_sum].sum().reset_index()

        # Calculate aggregated percentages
        aggregated_data['3P%'] = aggregated_data['3P'] / aggregated_data['3PA']
        aggregated_data['FG%'] = aggregated_data['FG'] / aggregated_data['FGA']

        # Replace NaN values with 0 for aggregated percentages
        aggregated_data['3P%'].fillna(0, inplace=True)
        aggregated_data['FG%'].fillna(0, inplace=True)

        # Save aggregated data
        aggregated_data.to_excel(writer, sheet_name='Aggregated Stats', index=False)

        # List of other stats to average (excluding '3P%' and 'FG%')
        stats_list = ['GAME_SCORE', 'PTS', 'TRB', 'AST', 'STL', 'BLK', 'TOV']

        # Generate and write averages for each stat
        for stat in stats_list:
            averages_df = combined_df.groupby(['Player', 'Year-Week'])[stat].mean().reset_index()
            averages_df.rename(columns={stat: f'Average {stat}'}, inplace=True)
            averages_df.to_excel(writer, sheet_name=f'Avg {stat}', index=False)

    print("Saved game logs and stats to Excel.")
else:
    print("No game logs were retrieved.")

########### Step 2: Calculate Week-over-Week Changes for Each Player ###########

import pandas as pd
import numpy as np

# Load the game logs
file_path = '/Users/tonysantoorjian/Documents/game_logs_and_stats.xlsx'  # Adjust this path
xls = pd.ExcelFile(file_path)
game_logs = pd.read_excel(xls, sheet_name='Game Logs')

# Calculate the true overall averages and sums for all stats
# For percentage stats (3P% and FG%)
game_logs['3P%'] = game_logs['3P'] / game_logs['3PA']
game_logs['FG%'] = game_logs['FG'] / game_logs['FGA']
# Replace NaN values with 0 for percentages
game_logs['3P%'].fillna(0, inplace=True)
game_logs['FG%'].fillna(0, inplace=True)

overall_averages = game_logs.groupby('Player').agg({
    '3P': 'sum',
    '3PA': 'sum',
    'FG': 'sum',
    'FGA': 'sum',
    'GAME_SCORE': 'mean',
    'PTS': 'mean',
    'TRB': 'mean',
    'AST': 'mean',
    'STL': 'mean',
    'BLK': 'mean',
    'TOV': 'mean'
})

# Calculate overall percentages after summing
overall_averages['3P%'] = overall_averages['3P'] / overall_averages['3PA']
overall_averages['FG%'] = overall_averages['FG'] / overall_averages['FGA']

# Replace NaN values with 0 for calculated percentages
overall_averages['3P%'].fillna(0, inplace=True)
overall_averages['FG%'].fillna(0, inplace=True)

# Ensure no division by zero
overall_averages.replace([np.inf, -np.inf], 0, inplace=True)

# Define the sheets to load for week-over-week comparison for other stats
sheets = ['Avg GAME_SCORE', 'Avg PTS', 'Avg TRB', 'Avg AST', 'Avg STL', 'Avg BLK', 'Avg TOV']
data = {sheet.split(' ')[1]: pd.read_excel(xls, sheet_name=sheet) for sheet in sheets}  # Use stat names as keys

# Initialize a dictionary to store all players' week-over-week changes
all_players_changes = {}

# Ask the user whether to use the most current week or not
use_current_week_input = input("Do you want to use the most current week for comparison? (yes/no): ").strip().lower()
use_current_week = use_current_week_input in ['yes', 'y']

# Load the "Aggregated Stats" sheet for percentage calculations
aggregated_stats = pd.read_excel(xls, sheet_name='Aggregated Stats')

# Process each player for week-over-week comparison using the aggregated stats for 3P% and FG%
for player, group in aggregated_stats.groupby('Player'):
    group_sorted = group.sort_values(by='Year-Week', ascending=True)
    for stat in ['3P%', 'FG%']:  # Only process 3P% and FG% here
        if use_current_week and len(group_sorted) > 1:
            # Use the most current week and the week before it
            current_week_data = group_sorted.iloc[-1]
            previous_week_data = group_sorted.iloc[-2]
        elif not use_current_week and len(group_sorted) > 2:
            # Skip the most current week
            current_week_data = group_sorted.iloc[-2]
            previous_week_data = group_sorted.iloc[-3]
        else:
            # Skip players without enough data
            continue

        # Calculate the changes for 3P% and FG%
        change_from_previous = current_week_data[stat] - previous_week_data[stat]
        change_from_average = current_week_data[stat] - overall_averages.loc[player, stat]

        # Store the changes and the week values for 3P% and FG%
        all_players_changes.setdefault(player, {})[stat] = {
            'Change from Previous': change_from_previous,
            'Change from Average': change_from_average,
            'Current Week Value': current_week_data[stat],
            'Previous Week Value': previous_week_data[stat],
            'Overall Average': overall_averages.loc[player, stat]
        }

# Process other stats
for stat, df in data.items():
    df_sorted = df.sort_values(by=['Player', 'Year-Week'], ascending=True)
    for player, group in df_sorted.groupby('Player'):
        if use_current_week and len(group) > 1:
            # Use the most current week and the week before it
            current_week_value = group.iloc[-1]['Average ' + stat]  # Corrected column name
            previous_week_value = group.iloc[-2]['Average ' + stat]  # Corrected column name
        elif not use_current_week and len(group) > 2:
            # Skip the most current week
            current_week_value = group.iloc[-2]['Average ' + stat]  # Corrected column name
            previous_week_value = group.iloc[-3]['Average ' + stat]  # Corrected column name
        else:
            # Skip players without enough data
            continue

        # Calculate the changes for other stats
        overall_average = overall_averages.loc[player, stat] if stat in overall_averages.columns else 0
        change_from_previous = current_week_value - previous_week_value
        change_from_average = current_week_value - overall_average

        # Store the changes and the week values for other stats
        all_players_changes.setdefault(player, {})[stat] = {
            'Change from Previous': change_from_previous,
            'Change from Average': change_from_average,
            'Current Week Value': current_week_value,
            'Previous Week Value': previous_week_value,
            'Overall Average': overall_average
        }

# Process each player for week-over-week comparison using the aggregated stats for 3P% and FG%
# for player, group in aggregated_stats.groupby('Player'):
#     group_sorted = group.sort_values(by='Date', ascending=True)
#     for stat in ['3P%', 'FG%']:  # Only process 3P% and FG% here
#         if len(group_sorted) > 5:
#             # Use the most current 5 games
#             current_games_data = group_sorted.tail(5)
#             previous_games_data = group_sorted.iloc[-6:-1]
#         else:
#             # Skip players without enough data
#             continue
#
#         # Calculate the changes for 3P% and FG%
#         change_from_previous = current_games_data[stat].mean() - previous_games_data[stat].mean()
#         change_from_average = current_games_data[stat].mean() - overall_averages.loc[player, stat]
#
#         # Store the changes and the week values for 3P% and FG%
#         all_players_changes.setdefault(player, {})[stat] = {
#             'Change from Previous': change_from_previous,
#             'Change from Average': change_from_average,
#             'Current Games Average': current_games_data[stat].mean(),
#             'Previous Games Average': previous_games_data[stat].mean(),
#             'Overall Average': overall_averages.loc[player, stat]
#         }
#
# # Process other stats
# for stat, df in data.items():
#     df_sorted = df.sort_values(by=['Player', 'Date'], ascending=True)
#     for player, group in df_sorted.groupby('Player'):
#         if len(group) > 5:
#             # Use the most current 5 games
#             current_games_value = group.tail(5)['Average ' + stat].mean()  # Corrected column name
#             previous_games_value = group.iloc[-6:-1]['Average ' + stat].mean()  # Corrected column name
#         else:
#             # Skip players without enough data
#             continue
#
#         # Calculate the changes for other stats
#         overall_average = overall_averages.loc[player, stat] if stat in overall_averages.columns else 0
#         change_from_previous = current_games_value - previous_games_value
#         change_from_average = current_games_value - overall_average
#
#         # Store the changes and the week values for other stats
#         all_players_changes.setdefault(player, {})[stat] = {
#             'Change from Previous': change_from_previous,
#             'Change from Average': change_from_average,
#             'Current Games Average': current_games_value,
#             'Previous Games Average': previous_games_value,
#             'Overall Average': overall_average
#         }

# Convert the dictionary to a DataFrame for easier Excel output
df_output = pd.DataFrame()
for player, stats in all_players_changes.items():
    for stat, details in stats.items():
        row = {'Player': player, 'Stat': stat, **details}
        df_output = df_output.append(row, ignore_index=True)

# Specify the output file path
output_file_path = '/Users/tonysantoorjian/Documents/week_over_week_changes.xlsx'  # Update the path as necessary
# Write the DataFrame to an Excel file
df_output.to_excel(output_file_path, index=False)

print(f"Week-over-week changes written to {output_file_path}")