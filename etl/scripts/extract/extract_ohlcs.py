import requests
import json
import datetime
import os
import logging
from info import API_OHCLS

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_ohlcs_data(date, adjusted, include_otc, api_key):
    """Fetch OHLCs data from the API."""
    
    url = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date}?adjusted={adjusted}&include_otc={include_otc}&apiKey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("results", [])
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"Error parsing JSON response: {e}")
        return []

def save_data_to_file(data, directory, filename):
    """Save data to a JSON file."""
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, filename)
    with open(path, "w") as outfile:
        json.dump(data, outfile, indent=4)
    logging.info(f"Data saved at {path}")

def extract_ohlcs_data():
    """Extract OHLCs data and save to file."""

    date_crawl = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    data = fetch_ohlcs_data(date_crawl, "true", "true", API_OHCLS)

    #define path and save to file
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")
    directory = "/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/raw/ohcls"
    filename = f"data_ohlcs_{date}.json"
    save_data_to_file(data, directory, filename)

    logging.info(f"The process of crawling {len(data)} OHLCs was successful")
    print(date)

if __name__ == "__main__":
    extract_ohlcs_data()
