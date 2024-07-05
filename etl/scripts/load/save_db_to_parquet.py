import pandas as pd
import datetime
import psycopg2

def get_sql_script(path):
    # Read the SQL file
    with open(path, 'r') as file:
        sql_script = file.read()
    return sql_script

def query_to_parquet(query, conn, parquet_file_path):
    # Execute the query and fetch the result into a DataFrame
    df = pd.read_sql_query(query, conn)
    print(df.info())
    print(df)
    
    # Save the DataFrame to a Parquet file
    df.to_parquet(parquet_file_path, engine='pyarrow')

def save_db_to_parquet():
    # Connection details for PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        database="testStock",
        user="postgres",
        password="12345678"
    )

    # Path to the SQL query file
    path = r"/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/scripts/load/db-scripts.sql"
    query = get_sql_script(path)
    
    # Path to the output Parquet file
    date = datetime.date.today().strftime("%Y_%m_%d")
    parquet_file_path = f"/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/completed/companies_parquet/data_companies_parquet_{date}.parquet"

    # Execute the query and save the result to a Parquet file
    query_to_parquet(query, conn, parquet_file_path)
    print(f"Saved data from database to parquet successfully at {parquet_file_path}")

    conn.close()

save_db_to_parquet()