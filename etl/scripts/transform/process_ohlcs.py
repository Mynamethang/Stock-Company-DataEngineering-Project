import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, lit, col
from datetime import datetime, timedelta
import pyarrow as pa
import pandas as pd

def process(hdfs_directory, database_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("ProcessToDataWarehouse") \
        .master("local[4]") \
        .config("spark.sql.caseSensitive", "true") \
        .getOrCreate() 

    # get latest ohcls file from ohcls directory
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet") # List all files in the directory
    files_df = files_df.withColumn("file_name", input_file_name()) # Extract file names and modification times
    latest_parquet_file_path = files_df.orderBy("modificationTime", ascending=False).select("file_name").first().file_name# Order files by modification time descending and get the latest file

    # Read Parquet file into Spark DataFrame
    df_spark = spark.read.parquet(latest_parquet_file_path)
    df_spark.printSchema()
    df_spark.show(5)
    
    # Connect to DuckDB
    conn = duckdb.connect(database=database_path)

    # Rename columns for clarity
    df_spark = df_spark.withColumnRenamed("T", "company_ticket") \
        .withColumnRenamed("v", "volume") \
        .withColumnRenamed("vw", "volume_weighted") \
        .withColumnRenamed("o", "open") \
        .withColumnRenamed("c", "close") \
        .withColumnRenamed("h", "high") \
        .withColumnRenamed("l", "low") \
        .withColumnRenamed("t", "time_stamp") \
        .withColumnRenamed("n", "num_of_trades") \
        .withColumnRenamed("otc", "is_otc")


    # Get yesterday's date
    yesterday = datetime.now().date() - timedelta(days=1)
    print(f"Yesterday's date: {yesterday}")

    # Connect to DuckDB
    conn = duckdb.connect(database=database_path)

    # Insert new time data into dim_time if it does not exist
    conn.execute(f'''
        INSERT INTO dim_time (date, day_of_week, month, quarter, year)
        SELECT '{yesterday}', '{yesterday.strftime("%A")}', '{yesterday.strftime("%B")}', '{((yesterday.month - 1) // 3) + 1}', {yesterday.year}
        WHERE NOT EXISTS (SELECT 1 FROM dim_time WHERE date = '{yesterday}')
    ''')

    # Get corresponding time_id from dim_time
    candles_time_id = conn.execute(f'''
        SELECT time_id FROM dim_time WHERE date = '{yesterday}'
    ''').fetchone()[0]

    # Get corresponding company_id and company_ticket from dim_companies
    id_company_df = conn.execute(f'''
        SELECT company_id, company_ticket FROM dim_companies
    ''').fetchdf()

    # Convert id_company_df to Spark DataFrame
    companies_spark_df = spark.createDataFrame(id_company_df)

    # Join companies_spark_df to df_spark to get corresponding company_id
    df_spark = df_spark.join(companies_spark_df, on='company_ticket', how='inner')

    # Add candles_time_id to DataFrame
    df_spark = df_spark.withColumn('candles_time_id', lit(candles_time_id))

    # Convert PySpark DataFrame to Pandas DataFrame
    df_pandas = df_spark.toPandas()

    # Insert data into fact_candles table
    conn.execute('''
        INSERT INTO fact_candles (
            candle_volume,
            candle_volume_weighted,
            candle_open,
            candle_close,
            candle_high,
            candle_low,
            candle_time_stamp,
            candle_num_of_trades,
            candle_is_otc,
            candles_time_id,
            candle_company_id
        ) SELECT 
            volume,
            volume_weighted,
            open,
            close,
            high,
            low,
            time_stamp,
            num_of_trades,
            is_otc,
            candles_time_id,
            company_id
        FROM df_pandas
    ''')

    print("Data inserted into fact_candles successfully!")

    # Close DuckDB connection
    conn.close()

    # Stop Spark session
    spark.stop()

def process_ohlcs_to_datawarehouse():
    hdfs_directory = "/stock-market-data/ohlcs/raw"
    database_path = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/datawarehouse.duckdb'

    process(hdfs_directory, database_path)

if __name__ == '__main__':
    process_ohlcs_to_datawarehouse()