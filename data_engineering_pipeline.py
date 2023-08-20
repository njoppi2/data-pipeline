from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd

default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'data_engineering_challenge',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Daily execution
    catchup=False,  # Do not backfill past dates
)

def extract_from_postgres():
    pass

def extract_from_csv():
    pass

def load_into_final_database():
    pass

# Define tasks
extract_postgres_task = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_from_csv',
    python_callable=extract_from_csv,
    dag=dag,
)

load_database_task = PythonOperator(
    task_id='load_into_final_database',
    python_callable=load_into_final_database,
    dag=dag,
)

# Task dependencies
[extract_postgres_task, extract_csv_task] >> load_database_task
