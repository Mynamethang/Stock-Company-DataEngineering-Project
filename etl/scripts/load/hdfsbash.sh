#!/bin/bash

# Variable to store the latest file names
latest_files=""

# Directory paths
local_directory=/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/completed/companies_parquet
hdfs_directory=/stock-market-data/companies/raw

Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# Directory paths
local_directory=/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/completed/ohcls_parquet
hdfs_directory=/stock-market-data/ohlcs/raw

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi
# done

# Directory paths
local_directory=/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/data/completed/news_parquet
hdfs_directory=/stock-market-data/news/raw

# Find the latest Parquet file
latest_file=$(ls -t "$local_directory"/*.parquet | head -1)

# Check if a file is found
if [ -z "$latest_file" ]; then
    echo "No Parquet file found in the directory $local_directory."
else
    # Upload the file to HDFS
    hdfs dfs -put "$latest_file" "$hdfs_directory"

    # Append the file name and HDFS directory to the variable
    latest_files="$latest_files$hdfs_directory/$(basename $latest_file)\n"
fi

# Print the latest file names (for Airflow XCom)
echo -e "$latest_files"

# bash /home/anhcu/Final_ETL_App/etl-app/elt/scripts/load/load_parquet_to_hdfs.sh