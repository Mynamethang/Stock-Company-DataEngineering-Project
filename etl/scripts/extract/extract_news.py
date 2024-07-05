import requests
import json
import datetime
import os
import logging
from info import API_NEWS

# Configure logging
#logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_data_by_time_range(time_zone):
    """Get data based on the current time zone range."""

    yesterday = (datetime.date.today() - datetime.timedelta(days=1))
    if time_zone == 1:
        time_from = yesterday.strftime("%Y%m%dT0000")
        time_to = yesterday.strftime("%Y%m%dT2359")
    elif time_zone == 2:
        time_from = yesterday.strftime("%Y%m%dT0000")
        time_to = yesterday.strftime("%Y%m%dT1200")
    else:
        time_from = yesterday.strftime("%Y%m%dT1201")
        time_to = yesterday.strftime("%Y%m%dT2359")

    return time_from, time_to

def fetch_news(time_from, time_to, function, sort, limit, api_key):
    """Fetch news data from the API."""

    url = f'https://www.alphavantage.co/query?function={function}&sort={sort}&time_from={time_from}&time_to={time_to}&limit={limit}&apikey={api_key}'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json().get("feed", [])
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


def extract_news_data():
    FUNCTION = "NEWS_SENTIMENT"
    SORT = "LATEST"
    LIMIT = "1000"

    all_news = []
    total = 0
    for time_zone in [1, 2, 3]:
        # Get the datetime range
        time_from, time_to = get_data_by_time_range(time_zone)
        logging.info(f"Fetching news from {time_from} to {time_to}")

        # Fetch news data from the API
        data = fetch_news(time_from, time_to, FUNCTION, SORT, LIMIT, API_NEWS)

        # Increment the total count of news items
        total += len(data)
        logging.info(f"Total news fetched so far: {total}")

        # Append the data to the all_news list
        all_news += data
        
        if total >= 1000:
            break

    # Get yesterday's date formatted as YYYY_MM_DD
    date = (datetime.date.today() - datetime.timedelta(days=1)).strftime("%Y_%m_%d")

    #define path
    directory = "/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/raw/news"
    filename = f"data_news_{date}.json"
    # Save the data to a file
    save_data_to_file(all_news, directory, filename)

    logging.info(f"The process of crawling {total} news was successful")

if __name__ == "__main__":
    extract_news_data()
