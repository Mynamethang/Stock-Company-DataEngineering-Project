import os
import requests
import time
import datetime
import logging
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)

def download_data():
    # Create the 'data' directory if it doesn't exist
    date_dataset = datetime.date.today().strftime("%Y_%m_%d")
    directory = f'data-for-dashboards/datasets/data_{date_dataset}'
    os.makedirs(directory, exist_ok=True)

    # Dictionary containing URL paths and their corresponding file names
    base_url = 'http://192.168.100.242:5000'
    urls_to_paths = {
        f'{base_url}/dim_time': f'{directory}/dim_time.csv',
        f'{base_url}/dim_companies': f'{directory}/dim_companies.csv',
        f'{base_url}/dim_topics': f'{directory}/dim_topics.csv',
        f'{base_url}/fact_news_companies': f'{directory}/fact_news_companies.csv',
        f'{base_url}/fact_news_topics': f'{directory}/fact_news_topics.csv',
        f'{base_url}/fact_candles': f'{directory}/fact_candles.csv',
    }

    # Loop through each URL and its corresponding file path
    for url, file_path in urls_to_paths.items():
        attempts = 0
        success = False

        # Try to download the data up to 3 times
        while attempts < 3 and not success:
            try:
                logging.info(f"Attempting to download data from {url} (Attempt {attempts + 1})")
                response = requests.get(url)
                response.raise_for_status()  # Raise an HTTPError for bad responses

                # Convert JSON content to pandas DataFrame
                data_json = response.json()
                df = pd.DataFrame(data_json)

                # Save DataFrame to CSV file
                df.to_csv(file_path, index=False)

                logging.info(f"Successfully downloaded data from {url} to {file_path}")
                success = True  # Mark success as True to exit the loop

            except requests.RequestException as e:
                logging.error(f"Error downloading data from {url}: {e}")
                attempts += 1
                if attempts < 3:
                    logging.info("Retrying in 30 seconds...")
                    time.sleep(30)  # Wait for 30 seconds before retrying

        if not success:
            logging.error(f"Failed to download data from {url} after 3 attempts.")

if __name__ == '__main__':
    download_data()
