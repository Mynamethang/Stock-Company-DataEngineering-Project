import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import glob

def get_latest_file(directory, extension):
    try:
        files = glob.glob(os.path.join(directory, f"*{extension}")) # find all files in directory
        if not files:
            return None
        latest_file = max(files, key=os.path.getmtime) # find the lastest file
        return latest_file
    except Exception as e:
        print(f"Error getting the latest file: {e}")
        return None

def load_json_from_file(filepath):
    try:
        with open(filepath, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data
    except Exception as e:
        print(f"Error loading JSON from file {filepath}: {e}")
        return None

def save_json_to_parquet(data, output_filepath):
    try:
        table = pa.Table.from_pandas(pd.DataFrame(data))
        pq.write_table(table, output_filepath)
    except Exception as e:
        print(f"Error saving to Parquet file {output_filepath}: {e}")

def load_api_to_dl(input_directory, output_directory):
    extension = '.json'
    latest_file = get_latest_file(input_directory, extension) # the latest file

    if latest_file:
        data = load_json_from_file(latest_file) # read and parse the JSON data from the latest file
        if data: 
            print(f"Read file: {latest_file}")
            filename = os.path.basename(latest_file).replace('.json', '.parquet') #replacing the .json extension with .parquet
            output_filepath = os.path.join(output_directory, filename) 
            save_json_to_parquet(data, output_filepath) # convert the JSON data to a Parquet file and save it  
            print(f"Saved Parquet file: {output_filepath}")
        else:
            print(f"Failed to load data from {latest_file}")
    else:
        print(f"No JSON files found in {input_directory}")

def save_api_to_parquet():
    """SAVE DATA IN PARQUET FORMAT"""

    try:
        # news data path
        input_directory = r'/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/raw/news'
        output_directory = r'/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/completed/news_parquet'
        os.makedirs(output_directory, exist_ok=True)
        load_api_to_dl(input_directory, output_directory)

        # ohcls data path
        input_directory = r'/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/raw/ohcls'
        output_directory = r'/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/completed/ohcls_parquet'
        os.makedirs(output_directory, exist_ok=True)
        load_api_to_dl(input_directory, output_directory)
    except Exception as e:
        print(f"Error in load_api_to_parquet: {e}")

save_api_to_parquet()