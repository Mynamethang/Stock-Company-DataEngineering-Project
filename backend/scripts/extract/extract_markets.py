import requests
from info import API_MARKET
import datetime
import json

# extract markets function
def extract_market_api():
    api = API_MARKET

    url = f'https://www.alphavantage.co/query?function=MARKET_STATUS&apikey={api}'
    response = requests.get(url)
    data_markets = response.json()['markets']


    date = datetime.date.today().strftime("%Y_%m_%d")
    path = f'/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/raw/markets/data_markets_{date}.json'

    # Open file in write mode (overwrite mode)
    if data_markets:
        with open(path, 'w') as file:
            json.dump(data_markets, file, indent=4)
            print("File written successfully.")
    else:
        print('Failed to retrieve data')