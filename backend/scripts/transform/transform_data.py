import os
import datetime
import json
import glob
import pandas as pd
import numpy as np

def get_latest_file(directory):
    """
    Get latest file from directory.
    
    :param dicrectory: The dicrectory to save data.
    :return: List for data dictionary.
    """

    # Get a list of all files in the directory
    files = glob.glob(os.path.join(directory, "*.json"))
    
    # Check if there are any files in the directory
    if not files:
        print(f"The directory {directory} is empty or contains no files.")
        return None
    
    # Get the latest file based on modification time
    latest_file = max(files, key=os.path.getmtime)
    print(f"The latest file is: {directory}")

    # read the lastest file
    if latest_file:
        with open(latest_file, 'r') as file:
            data_json = json.load(file)
        print(f"Transforming from file: {latest_file}")
    else:
        print("No file found")
        data_json = []
    
    return data_json

def cleaned_dataframe(dataframe):
    """
    Clean a DataFrame by replacing empty strings with NaN, dropping duplicates, and dropping rows with NaN.
    
    :param dataframe: The DataFrame to clean.
    :return: Cleaned DataFrame.
    """
    return dataframe.replace(r'^\s*$', np.nan, regex=True).drop_duplicates().dropna()

def save_to_json(dataframe, filename):
    """
    Save a DataFrame to a JSON file.
    
    :param dataframe: The DataFrame to save.
    :param filename: The path to the JSON file.
    """
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    dataframe.to_json(filename, orient='records', lines=True)
    print(f"Saved dataframe to {filename}")  


def transform_data():

    """Stage1. PARTITION DATA"""

    #get json data
    companies = get_latest_file('/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/raw/companies')
    markets = get_latest_file('/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/raw/markets')
    
    transform_directory = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed'

     # Get the current date for filenames
    date = datetime.date.today().strftime("%Y_%m_%d")

    regions = pd.DataFrame([
        {
            "region_name": item["region"],
            "region_local_open": item["local_open"],
            "region_local_close": item["local_close"]
        }
        for item in markets
    ])

    # Create a DataFrame for  data
    industries = pd.DataFrame([
        {
            "industry_name": item["industry"],
            "industry_sector": item["sector"]
        }
        for item in companies
    ])

    # Create a DataFrame for SIC industries data
    sic_industries = pd.DataFrame([
        {
            "sic_id": item["sic"],
            "sic_industry": item["sicIndustry"],
            "sic_sector": item["sicSector"]
        }
        for item in companies
    ])

    # Create a DataFrame for exchanges 
    exchanges = pd.DataFrame([
        {
            "region": item["region"],
            "exchange_name": item["primary_exchanges"]
        }
        for item in markets
    ])
    exchanges['exchange_name'] = exchanges['exchange_name'].str.split(', ')

    # Explode the 'primary_exchanges' column into multiple rows
    exchanges = exchanges.explode('exchange_name').reset_index(drop=True)

    # Create a DataFrame for companies
    companies = pd.DataFrame([
        {
            "company_id": item["id"],
            "company_exchange": item["exchange"], 
            "company_industry": item["industry"], 
            "company_sector": item["sector"], 
            "company_sic_id": item["sic"], 
            "company_name": item["name"], 
            "company_ticket": item["ticker"], 
            "company_is_delisted": item["isDelisted"], 
            "company_category": item["category"], 
            "company_currency": item["currency"], 
            "company_location": item["location"]
        }
        for item in companies
    ])

     # Filter companies DataFrame for specific exchanges and currency
    companies = companies[companies["company_exchange"].isin(["NYSE", "NASDAQ"])]
    companies = companies[companies["company_currency"] == "USD"]


    """STAGE 2. CLEAN AND SAVE PARTITIONED DATA"""

    #SAVE PATH
    regions_path = f"{transform_directory}/transformed_to_database_regions/transformed_regions_{date}.json"
    regions_path = f"{transform_directory}/transformed_to_database_regions/transformed_regions_{date}.json"
    industries_path = f"{transform_directory}/transformed_to_database_industries/transformed_industries_{date}.json"
    sic_industries_path = f"{transform_directory}/transformed_to_database_sic_industries/transformed_sic_industries_{date}.json"
    exchanges_path = f'{transform_directory}/transformed_to_database_exchanges/transformed_exchanges_{date}.json'
    companies_path = f'{transform_directory}/transformed_to_database_companies/transformed_companies_{date}.json'

    data_info = [
        (regions, regions_path),
        (industries, industries_path),
        (sic_industries, sic_industries_path),
        (exchanges, exchanges_path),
        (companies, companies_path)
    ]

    for df, path in data_info:
        df = cleaned_dataframe(df)
        save_to_json(df, path)
        print("Succesfully saved")











    


