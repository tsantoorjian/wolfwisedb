import requests

url = 'https://balldontlie.io/api/v1/stats?player_ids[]=237&seasons[]=2022'

url = 'https://www.balldontlie.io/api/v1/players?search=anthony_edwards'

url = 'https://www.balldontlie.io/api/v1/stats??seasons[]=2023&&player_ids[]=3547238&postseason=true'

response = requests.get(url)

result = response.json()