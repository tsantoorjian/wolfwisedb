import pandas as pd
from nba_api.stats.endpoints import leaguedashlineups

# Fetch data using nba_api
lineup_data = leaguedashlineups.LeagueDashLineups(
    measure_type_detailed_defense='Advanced',
    per_mode_detailed='Totals',
    group_quantity = 3,
    season='2024-25',
    season_type_all_star='Regular Season',
    team_id_nullable='1610612750',  # Minnesota Timberwolves Team ID
)

# Convert to DataFrame
df = lineup_data.get_data_frames()[0]

# Filter for lineups with at least 50 minutes played
df = df[df["MIN"] >= 50]

# Sort by Net Rating and get top 3 and bottom 3
best_lineups = df.nlargest(3, "NET_RATING")
worst_lineups = df.nsmallest(3, "NET_RATING")

# Combine the results
result_df = pd.concat([best_lineups, worst_lineups])

# Save to CSV
csv_filename = "min_timberwolves_lineups.csv"
result_df.to_csv(csv_filename, index=False)

print(f"CSV file saved: {csv_filename}")
