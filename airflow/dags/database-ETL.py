from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from datetime import timedelta
import sys
import subprocess
import os

# Add the paths to sys.path
sys.path.append('/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/scripts/extract')
sys.path.append('/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/scripts/transform')
sys.path.append('/home/ngocthang/Documents/Code/Stock-Company-Analysis/backend/scripts/load')

from extract_companies import extract_company_api
from extract_markets import extract_market_api
from transform_data import transform_data
from load_to_temp_tables import load_to_temp_tables
from load_to_main_tables import load_to_main_tables

"""
task1 : extract_company_api
task2 : extract_market_api
task3 : transfrorm_data
task4 : load_to_temp_tables
task5 : load_to_main_tables
"""


default_args = {
    'owner': 'ngocthang',
    'depends_on_past': False,
    'email': ['nguyenngocthang0939@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Khởi tạo DAG
with DAG(
    dag_id='ETL_to_Database',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='1 0 * * *',
    start_date=days_ago(1),
    catchup=False,
) as dag:
    

    # Task 1: Crawl companies
    db_extract_company_task = PythonOperator(
        task_id='db_extract_company_api',
        python_callable=extract_company_api,
    )

    # Task 2: Crawl markets
    db_extract_market_task = PythonOperator(
        task_id='db_extract_market_api',
        python_callable=extract_market_api,
    )

    # Task 3: Transform to database 1
    db_transform_task = PythonOperator(
        task_id='db_transform_data',
        python_callable=transform_data,
    )

    # Task 4: Load JSON to DB 1
    db_load_temp_tables_task = PythonOperator(
        task_id='db_load_temp_tables',
        python_callable=load_to_temp_tables,
    )

    # Task 5: Transform to database 2
    db_load_main_tables_task = PythonOperator(
        task_id='db_load_main_tables',
        python_callable=load_to_main_tables,
    )

    # task
    [db_extract_company_task, db_extract_market_task] >> db_transform_task >> db_load_temp_tables_task >> db_load_main_tables_task 