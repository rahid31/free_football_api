import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

url = "https://free-api-live-football-data.p.rapidapi.com/football-get-all-matches-by-league"

params = {"leagueid":"47"}

headers = {
	"x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
	"x-rapidapi-host": os.getenv("RAPIDAPI_HOST")
}

response = requests.get(url, headers=headers, params=params)

data = response.json().get('response', {}).get('matches', [])

df = pd.DataFrame(data)
print(df)