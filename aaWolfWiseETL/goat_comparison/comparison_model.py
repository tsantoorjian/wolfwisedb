import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import os

# ----------------------------
# 1. Create Sample Data
# ----------------------------
data = [
    # Anthony Edwards: 5 seasons
    {"Player": "Anthony Edwards", "Season": 1, "Points": 15, "Assists": 3, "Rebounds": 4, "FG%": 0.45},
    {"Player": "Anthony Edwards", "Season": 2, "Points": 18, "Assists": 4, "Rebounds": 5, "FG%": 0.46},
    {"Player": "Anthony Edwards", "Season": 3, "Points": 20, "Assists": 5, "Rebounds": 6, "FG%": 0.47},
    {"Player": "Anthony Edwards", "Season": 4, "Points": 24, "Assists": 6, "Rebounds": 7, "FG%": 0.48},
    {"Player": "Anthony Edwards", "Season": 5, "Points": 28, "Assists": 7, "Rebounds": 8, "FG%": 0.50},
    
    # Michael Jordan: 6 seasons (we will truncate to 5)
    {"Player": "Michael Jordan", "Season": 1, "Points": 20, "Assists": 2, "Rebounds": 5, "FG%": 0.45},
    {"Player": "Michael Jordan", "Season": 2, "Points": 22, "Assists": 3, "Rebounds": 6, "FG%": 0.46},
    {"Player": "Michael Jordan", "Season": 3, "Points": 24, "Assists": 3, "Rebounds": 7, "FG%": 0.47},
    {"Player": "Michael Jordan", "Season": 4, "Points": 26, "Assists": 4, "Rebounds": 8, "FG%": 0.48},
    {"Player": "Michael Jordan", "Season": 5, "Points": 28, "Assists": 4, "Rebounds": 9, "FG%": 0.49},
    {"Player": "Michael Jordan", "Season": 6, "Points": 30, "Assists": 5, "Rebounds": 10, "FG%": 0.50},
    
    # Player X: 5 seasons
    {"Player": "Player X", "Season": 1, "Points": 10, "Assists": 4, "Rebounds": 3, "FG%": 0.40},
    {"Player": "Player X", "Season": 2, "Points": 12, "Assists": 5, "Rebounds": 4, "FG%": 0.41},
    {"Player": "Player X", "Season": 3, "Points": 14, "Assists": 6, "Rebounds": 5, "FG%": 0.42},
    {"Player": "Player X", "Season": 4, "Points": 16, "Assists": 7, "Rebounds": 6, "FG%": 0.43},
    {"Player": "Player X", "Season": 5, "Points": 18, "Assists": 8, "Rebounds": 7, "FG%": 0.44},
]

df = pd.DataFrame(data)

# ----------------------------
# 2. Define the Similarity Function
# ----------------------------
def compute_similarity(selected_player_data, candidate_player_data, stats_groups, weight=0.5):
    """
    Computes similarity score between selected player and candidate player's first N seasons
    """
    N = len(selected_player_data)
    
    if len(candidate_player_data) < N:
        return None
        
    candidate_data = candidate_player_data.head(N)
    
    # Calculate similarity for each stat group
    similarity_components = {}
    
    for group_name, stats in stats_groups.items():
        # Combine and normalize data for this group
        combined = pd.concat([selected_player_data, candidate_data])
        scaler = StandardScaler()
        combined_scaled = combined.copy()
        combined_scaled[stats] = scaler.fit_transform(combined[stats])
        
        # Split back into selected and candidate data
        selected_scaled = combined_scaled.iloc[:N]
        candidate_scaled = combined_scaled.iloc[N:]
        
        # Performance similarity (season by season)
        performance_diffs = []
        for i in range(N):
            sel_vector = selected_scaled.iloc[i][stats].values.astype(float)
            cand_vector = candidate_scaled.iloc[i][stats].values.astype(float)
            dist = np.linalg.norm(sel_vector - cand_vector)
            performance_diffs.append(dist)
        performance_distance = np.mean(performance_diffs)
        
        # Growth similarity
        growth_diffs = []
        for stat in stats:
            growth_sel = selected_scaled.iloc[-1][stat] - selected_scaled.iloc[0][stat]
            growth_cand = candidate_scaled.iloc[-1][stat] - candidate_scaled.iloc[0][stat]
            growth_diff = abs(growth_sel - growth_cand)
            growth_diffs.append(growth_diff)
        growth_distance = np.mean(growth_diffs)
        
        # Combined score for this group
        group_score = weight * performance_distance + (1 - weight) * growth_distance
        
        similarity_components[f"{group_name}_perf"] = performance_distance
        similarity_components[f"{group_name}_growth"] = growth_distance
        similarity_components[f"{group_name}_combined"] = group_score
    
    # Calculate weighted overall similarity score
    # Advanced stats get 50% weight, basic and shooting each get 25%
    overall_score = (
        0.25 * similarity_components['basic_combined'] +
        0.25 * similarity_components['shooting_combined'] +
        0.50 * similarity_components['advanced_combined']
    )
    similarity_components['overall'] = overall_score
    
    return similarity_components

def print_similarity_results(sorted_results, comparison_type, num_seasons):
    """Helper function to print similarity results in a formatted way"""
    print(f"\nMost similar {comparison_type} to Anthony Edwards (first {num_seasons} seasons):")
    print("\nDetailed breakdown (lower scores = more similar):")
    print(f"{'Player':<25} {'Basic':<20} {'Shooting':<20} {'Advanced':<20} {'Overall':<10}")
    print("-" * 95)
    
    for player, scores in sorted_results[:10]:
        basic_score = scores['basic_combined']
        shooting_score = scores['shooting_combined']
        advanced_score = scores['advanced_combined']
        overall = scores['overall']
        
        print(f"{player:<25} {basic_score:>6.3f} ({scores['basic_growth']:>5.3f}) "
              f"{shooting_score:>6.3f} ({scores['shooting_growth']:>5.3f}) "
              f"{advanced_score:>6.3f} ({scores['advanced_growth']:>5.3f}) "
              f"{overall:>6.3f}")

def get_valid_players(df, player_data, stats_groups):
    """Helper function to get valid players for comparison"""
    valid_players = []
    # Get all required stats
    all_stats = [stat for group in stats_groups.values() for stat in group]
    num_seasons = len(player_data)
    
    for player in df['PLAYER_NAME'].unique():
        # Get first N seasons for this player
        player_stats = df[df['PLAYER_NAME'] == player].sort_values('SEASON_NUMBER')
        
        # Check if player has enough consecutive seasons
        if len(player_stats) < num_seasons:
            continue
            
        # Get exactly the first N seasons
        first_n_seasons = player_stats.head(num_seasons)
        
        # Check if seasons are consecutive (no gaps)
        expected_seasons = list(range(1, num_seasons + 1))
        actual_seasons = first_n_seasons['SEASON_NUMBER'].tolist()
        if actual_seasons != expected_seasons:
            continue
            
        # Check if all required stats are present and non-null
        if first_n_seasons[all_stats].isnull().any().any():
            continue
            
        # Check if all stats have valid numeric values (not inf or -inf)
        if (first_n_seasons[all_stats].isin([float('inf'), float('-inf')])).any().any():
            continue
            
        # If all checks pass, add player to valid list
        valid_players.append(player)
    
    return valid_players

def save_all_comparisons(all_comparisons, output_dir='career_stats'):
    """Save all player comparisons to a single CSV file"""
    # Create output directory if it doesn't exist
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        
    # Convert all comparisons to DataFrame rows
    all_rows = []
    for player_name, (nba_results, hof_results, num_seasons) in all_comparisons.items():
        # Add NBA comparisons
        for comp_player, scores in nba_results[:10]:
            row = {
                'player_analyzed': player_name,
                'seasons_compared': num_seasons,
                'comparison_player': comp_player,
                'comparison_type': 'current_nba',
                'overall_similarity': scores['overall'],
                'basic_combined': scores['basic_combined'],
                'basic_performance': scores['basic_perf'],
                'basic_growth': scores['basic_growth'],
                'shooting_combined': scores['shooting_combined'],
                'shooting_performance': scores['shooting_perf'],
                'shooting_growth': scores['shooting_growth'],
                'advanced_combined': scores['advanced_combined'],
                'advanced_performance': scores['advanced_perf'],
                'advanced_growth': scores['advanced_growth']
            }
            all_rows.append(row)
        
        # Add HOF comparisons
        for comp_player, scores in hof_results[:10]:
            row = {
                'player_analyzed': player_name,
                'seasons_compared': num_seasons,
                'comparison_player': comp_player,
                'comparison_type': 'hof',
                'overall_similarity': scores['overall'],
                'basic_combined': scores['basic_combined'],
                'basic_performance': scores['basic_perf'],
                'basic_growth': scores['basic_growth'],
                'shooting_combined': scores['shooting_combined'],
                'shooting_performance': scores['shooting_perf'],
                'shooting_growth': scores['shooting_growth'],
                'advanced_combined': scores['advanced_combined'],
                'advanced_performance': scores['advanced_perf'],
                'advanced_growth': scores['advanced_growth']
            }
            all_rows.append(row)
    
    # Create DataFrame and save to CSV
    df = pd.DataFrame(all_rows)
    filename = os.path.join(output_dir, 'all_player_comparisons.csv')
    df.to_csv(filename, index=False)
    
    return filename

def main():
    # Read the CSV files
    wolves_df = pd.read_csv('career_stats/wolves_all_players_career_stats.csv')
    nba_df = pd.read_csv('career_stats/nba_all_players_career_stats.csv')
    hof_df = pd.read_csv('career_stats/hof_players_career_stats.csv')
    
    # Group stats by category
    stats_groups = {
        'basic': ['MIN_PER_GAME', 'PTS_PER_GAME', 'AST_PER_GAME', 'REB_PER_GAME',
                 'STL_PER_GAME', 'BLK_PER_GAME', 'TOV_PER_GAME'],
        'shooting': ['FG_PCT', 'FG3_PCT', 'FT_PCT', 'TS_PCT'],
        'advanced': ['USG_PCT', 'AST_PCT', 'BPM', 'WS_PER_48', 'VORP']
    }

    # Process each Wolves player
    wolves_players = wolves_df['PLAYER_NAME'].unique()
    print(f"\nAnalyzing {len(wolves_players)} Wolves players...")
    
    # Dictionary to store all comparisons
    all_comparisons = {}
    
    for player_name in wolves_players:
        # Get player's data
        player_data = wolves_df[wolves_df['PLAYER_NAME'] == player_name].sort_values('SEASON_NUMBER')
        num_seasons = len(player_data)
        
        print(f"\nAnalyzing {player_name}'s first {num_seasons} seasons...")
        
        # Skip if player doesn't have enough stats
        if not all(stat in player_data.columns for group in stats_groups.values() for stat in group):
            print(f"Skipping {player_name} - missing required stats")
            continue
            
        # Process current NBA players
        nba_similarity_results = {}
        valid_nba_players = get_valid_players(nba_df, player_data, stats_groups)
        print(f"Found {len(valid_nba_players)} current NBA players with complete stats")
        
        for comp_player in valid_nba_players:
            if comp_player != player_name:  # Skip comparing with self
                comp_data = nba_df[nba_df['PLAYER_NAME'] == comp_player].sort_values('SEASON_NUMBER')
                components = compute_similarity(player_data, comp_data, stats_groups, weight=0.8)
                if components is not None:
                    nba_similarity_results[comp_player] = components
        
        # Process HOF players
        hof_similarity_results = {}
        valid_hof_players = get_valid_players(hof_df, player_data, stats_groups)
        print(f"Found {len(valid_hof_players)} HOF players with complete stats")
        
        for comp_player in valid_hof_players:
            comp_data = hof_df[hof_df['PLAYER_NAME'] == comp_player].sort_values('SEASON_NUMBER')
            components = compute_similarity(player_data, comp_data, stats_groups, weight=0.8)
            if components is not None:
                hof_similarity_results[comp_player] = components

        # Sort results
        sorted_nba_results = sorted(nba_similarity_results.items(), key=lambda x: x[1]['overall'])
        sorted_hof_results = sorted(hof_similarity_results.items(), key=lambda x: x[1]['overall'])
        
        # Print results
        print_similarity_results(sorted_nba_results, "current NBA players", num_seasons)
        print_similarity_results(sorted_hof_results, "Hall of Fame players", num_seasons)
        
        # Store results
        all_comparisons[player_name] = (sorted_nba_results, sorted_hof_results, num_seasons)
    
    # Save all results to a single CSV
    output_file = save_all_comparisons(all_comparisons)
    print(f"\nAnalysis complete! All results saved to: {output_file}")

if __name__ == "__main__":
    main()
