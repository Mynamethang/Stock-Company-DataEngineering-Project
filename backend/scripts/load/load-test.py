from sqlalchemy import create_engine
import os
import glob
import pandas as pd
import psycopg2

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
    return latest_file



def insert_data(file_path, conflict_columns, table_name, conn, cur):
    """
    Insert data from a JSON file into a PostgreSQL table.
    
    :param file_path: Path to the JSON file.
    :param table_name: Name of the PostgreSQL table.
    :param conflict_columns: List of columns to check for conflicts.
    :param conn: PostgreSQL connection.
    :param conn: PostgreSQL cursor.
    """

    # get latest json file
    file = get_latest_file(file_path)
    if not file:
        print(f"No data found in {file_path}")
        return None
    
    # create dataframe from lates file
    dataframe = pd.read_json(file, lines=True)
    columns = list(dataframe.columns)

    #format columns and values
    values = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    conflict_columns_str = ', '.join(conflict_columns)
    
    #insert data
    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({values})
        ON CONFLICT ({conflict_columns_str}) DO NOTHING
    """
    
    for i, row in dataframe.iterrows():
        cur.execute(query, list(row))

    print(f"Inserted data into {table_name}")



# connect to PostgreSQL database
conn = psycopg2.connect("host=localhost dbname=testStock user=postgres password=12345678")
cur = conn.cursor() 

#file paths
path_industries = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_industries'
path_regions = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_regions'
path_sic_industries = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_sic_industries'

#conflict columns
conflict_column_industries = ['industry_name', 'industry_sector']
conflict_column_regions = ['region_name']
conflict_column_sic_industries= ['sic_industry', 'sic_sector']


params = [
    (path_industries, conflict_column_industries, 'industries'),
    (path_regions, conflict_column_regions, 'regions'),
    (path_sic_industries, conflict_column_sic_industries, 'sic_industries')
]

for path, col, name in params:
    insert_data(path, col, name, conn, cur)
    conn.commit()

#close connection
cur.close()
conn.close()



