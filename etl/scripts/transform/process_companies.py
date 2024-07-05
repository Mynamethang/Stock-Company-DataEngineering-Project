import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import pyarrow as pa


# def get_latest_parquet_file(hdfs_directory):
#     # Initialize Spark session
#     spark = SparkSession.builder \
#         .appName("GetLatestParquetFile") \
#         .master("local[4]") \
#         .getOrCreate()

#     # List all files in the directory
#     files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")

#     # Extract file names and modification times
#     files_df = files_df.withColumn("file_name", input_file_name())

#     # Order files by modification time descending and get the latest file
#     latest_file = files_df.orderBy("modificationTime", ascending=False).limit(1).collect()[0].file_name

#     spark.stop()
#     return latest_file

def process(hdfs_directory, database_path):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Process Companies") \
        .master("local[4]") \
        .getOrCreate()


    #extract the latest file
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")# List all files in the directory
    files_df = files_df.withColumn("file_name", input_file_name()) # Extract file names and modification times
    parquet_latest_file = files_df.orderBy("modificationTime", ascending=False).limit(1).collect()[0].file_name # Order files by modification time descending and get the latest file

    # Read Parquet file into PySpark DataFrame
    df_spark = spark.read.parquet(parquet_latest_file)

    # Display schema and a few rows of data
    df_spark.printSchema()
    df_spark.show()

    # Convert PySpark DataFrame to Arrow Table
    arrow_table = df_spark.toPandas()

    # Connect to DuckDB
    conn = duckdb.connect(database=database_path)

    # Register the Arrow Table in DuckDB
    conn.register("arrow_table", arrow_table) 

    # Insert data from Arrow Table into DuckDB table
    conn.execute('''
        INSERT INTO dim_companies (
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        ) SELECT 
            company_name,
            company_ticket,
            company_is_delisted,
            company_category,
            company_currency,
            company_location,
            company_exchange_name,
            company_region_name,
            company_industry_name,
            company_industry_sector,
            company_sic_industry,
            company_sic_sector
        FROM arrow_table
    ''')

    # Close DuckDB connection
    conn.close()
    
    # Stop Spark session
    spark.stop()

    print("Data has been successfully inserted into dim_companies in DuckDB!")

def process_companies_to_datawarehouse():
    hdfs_directory = '/stock-market-data/companies/raw' # hdfs path
    database_path = '/home/ngocthang/Documents/Code/Stock-Company-Analysis/warehouse/datawarehouse.duckdb' # Path to DuckDB database file

    process(hdfs_directory, database_path)

if __name__ == '__main__':
    process_companies_to_datawarehouse()
    