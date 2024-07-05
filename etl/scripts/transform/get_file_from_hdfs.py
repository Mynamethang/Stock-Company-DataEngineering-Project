import sys
import duckdb
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import pyarrow as pa

def get_latest_parquet_file(hdfs_directory):
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("GetLatestParquetFile") \
        .master("local[4]") \
        .getOrCreate()

    # List all files in the directory
    files_df = spark.read.format("binaryFile").load(hdfs_directory + "/*.parquet")

    # Extract file names and modification times
    files_df = files_df.withColumn("file_name", input_file_name())

    # Order files by modification time descending and get the latest file
    latest_file = files_df.orderBy("modificationTime", ascending=False).limit(1).collect()[0].file_name

    spark.stop()
    return latest_file


