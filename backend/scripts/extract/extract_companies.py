import requests
import json
from info import API_COMPANIES
from sec_api import MappingApi
import datetime

def extract_company_api():
    # create mapping API 
    mappingApi = MappingApi(api_key=API_COMPANIES)

    # 2 stock markets  which we will extract data from
    stock_markets = ['NASDAQ', 'NYSE']

    data_companies = []
    for market in stock_markets:
        market_listings_json = mappingApi.resolve('exchange', f'{market}')
        data_companies.extend(market_listings_json)
        print(f'Extract data from {market} market successfully')

    date = datetime.date.today().strftime("%Y_%m_%d")
    path = f'/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/raw/companies/data_companies_{date}.json'

    # Open file in write mode (overwrite mode)
    with open(path, 'w') as file:
        json.dump(data_companies, file, indent=4)
        print("File written successfully.")


