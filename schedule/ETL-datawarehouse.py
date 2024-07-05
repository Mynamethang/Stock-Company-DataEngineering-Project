from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import subprocess
import os

# Add the paths to sys.path
sys.path.append('/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/scripts/extract')
sys.path.append('/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/scripts/load')
sys.path.append('/home/ngocthang/Documents/Code/Stock-Company-Analysis/etl/scripts/transform')
from extract_news import extract_news_data
from extract_ohlcs import extract_ohlcs_data

from save_api_to_parquet import save_api_to_parquet
from save_db_to_parquet import save_db_to_parquet
from load_partquet_to_hdfs import load_to_hdfs

from process_companies import process_companies_to_datawarehouse
from process_news import process_news_to_datawarehouse
from process_ohlcs import process_ohlcs_to_datawarehouse

default_args = {
    'owner': 'ngocthang',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='ELT_to_Data_Warehouse',
    default_args=default_args,
    description='ETL DAG for Data Warehouse',
    schedule_interval='1 1 * * *',  # Chạy khi được kích hoạt bởi DAG khác
    start_date=days_ago(1),
    catchup=False,
) as dag:
    crawl_news_task = PythonOperator(
        task_id='crawl_news',
        python_callable=extract_news_data,
    )

    crawl_ohlcs_task = PythonOperator(
        task_id='crawl_ohlcs',
        python_callable=extract_ohlcs_data,
    )

    load_api_to_parquet_task = PythonOperator(
        task_id='load_api_to_parquet',
        python_callable=save_api_to_parquet,
    )

    load_db_to_parquet_task = PythonOperator(
        task_id='load_db_to_parquet',
        python_callable=save_db_to_parquet,
    )

    load_parquet_to_hdfs_task = PythonOperator(
        task_id='load_parquet_to_hdfs',
        python_callable=load_to_hdfs
    )

    process_companies_task = PythonOperator(
        task_id='process_companies',
        python_callable=process_companies_to_datawarehouse,
    )
    
    process_ohlcs_task = PythonOperator(
        task_id='process_ohlcs',
        python_callable=process_ohlcs_to_datawarehouse,
    )
    
    process_news_task = PythonOperator(
        task_id='process_news',
        python_callable=process_news_to_datawarehouse,
    )

    # Định nghĩa thứ tự chạy các task
    crawl_news_task >> load_api_to_parquet_task
    crawl_ohlcs_task >> load_api_to_parquet_task
    crawl_news_task >> load_db_to_parquet_task
    crawl_ohlcs_task >> load_db_to_parquet_task
    [load_api_to_parquet_task, load_db_to_parquet_task] >> load_parquet_to_hdfs_task
    load_parquet_to_hdfs_task >> process_companies_task >> process_ohlcs_task >> process_news_task