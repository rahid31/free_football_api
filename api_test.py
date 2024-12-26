import requests
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

url = "https://free-api-live-football-data.p.rapidapi.com/football-get-all-transfers"

params = {"page": "4"
          }

headers = {
	"x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
	"x-rapidapi-host": os.getenv("RAPIDAPI_HOST")
}

response = requests.get(url, headers=headers, params=params if params else None)

data = response.json().get('response', {}).get('transfers', [])

df = pd.DataFrame(data)
print(df)