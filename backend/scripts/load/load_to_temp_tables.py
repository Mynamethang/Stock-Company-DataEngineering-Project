import psycopg2
import os
import glob
import pandas as pd

def get_latest_file(directory):
    """
    Get latest file from directory.
    
    :param directory: The directory to save data.
    :return: The latest file path.
    """
    files = glob.glob(os.path.join(directory, "*.json"))
    
    if not files:
        print(f"The directory {directory} is empty or contains no files.")
        return None
    
    latest_file = max(files, key=os.path.getmtime)
    return latest_file

def insert_to_temp_tables(conn, cur, dataframe, table_name):
    """
    Insert Data
    :param conn: postgreSQL connection
    :param cur: postgreSQl cursor
    :param table_name: name of table
    """

    columns = list(dataframe.columns)
    values = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    
    query = f"""
        INSERT INTO {table_name} ({columns_str})
        VALUES ({values})
    """

    for _, row in dataframe.iterrows():
        cur.execute(query, tuple(row))
        conn.commit()



def load_to_temp_tables():
    """LOAD DATA INTO TEMP TABLES"""

    conn = psycopg2.connect(
        host="localhost",
        database="testStock",
        user="postgres",
        password="12345678"
    )
    cur = conn.cursor()
    

    path_companies = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_companies'
    path_exchanges = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_exchanges'
    path_industries = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_industries'
    path_regions = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_regions'
    path_sic_industries = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/data/transformed/transformed_to_database_sic_industries'

    tables = {
        path_companies: 'temp_companies',
        path_exchanges: 'temp_exchanges',
        path_industries: 'temp_industries',
        path_regions: 'temp_regions',
        path_sic_industries: 'temp_sic_industries'
    }

    for path, table in tables.items():
        file = get_latest_file(path)
        if file:
            df = pd.read_json(file, lines=True)
            insert_to_temp_tables(conn, cur, df, table)
            print(f"Data appended successfully to PostgreSQL table {table}")
        else:
            print(f"No file found in {path}")

    cur.close()
    conn.close()