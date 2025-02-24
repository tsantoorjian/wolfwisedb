import nba_scraper.nba_scraper as ns

#scrape a season
nba_df = ns.scrape_season(2019)

# if you want a csv if you don't pass a file path the default is home
# directory
ns.scrape_season(2024, data_format='csv', data_dir='/Users/tonysantoorjian/Documents/')