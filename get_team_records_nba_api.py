from nba_api.stats.endpoints import teamyearbyyearstats
import pandas as pd

# Timberwolves team ID
team_id = 1610612750

# Fetch team stats
team_stats = teamyearbyyearstats.TeamYearByYearStats(team_id=team_id)
df = team_stats.get_data_frames()[0]

# Display the DataFrame
print(df)
