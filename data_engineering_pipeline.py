from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os
import pandas as pd
from decouple import config
import psycopg2
from psycopg2 import sql
from utils.functions import get_date
import shutil
import csv
import io
from sqlalchemy import create_engine


db_params = {
    'dbname': config('DB_NAME'),
    'user': config('DB_USER'),
    'password': config('DB_PASSWORD'),
    'host': config('DB_HOST'),
    'port': config('DB_PORT'),
}

engine = create_engine(f'postgresql://{config("DB_USER")}:{config("DB_PASSWORD")}@{config("DB_HOST")}:{config("DB_PORT")}/{config("OUTPUT_DB_NAME")}')


def extract_from_postgres(date=get_date()):
    # Establish a connection to the Postgres database
    connection = psycopg2.connect(**db_params)

    # Get a list of all tables in the database
    cursor = connection.cursor()
    cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    tables = cursor.fetchall()
    cursor.close()

    # Define a list to store the names of the tables to be extracted
    stored_tables = []

    # Create a directory to store the CSV files for this extraction
    for raw_table_name in tables:
        table_name = raw_table_name[0]
        extraction_dir = f'data/postgres/{table_name}/{date}'

        # Remove the directory if it exists, then create it
        if os.path.exists(extraction_dir):
            shutil.rmtree(extraction_dir)
        os.makedirs(extraction_dir)

        # Define the SQL query to extract all data from the current table
        query = f"SELECT * FROM {table_name};"

        # Read data from the current table into a Pandas DataFrame
        df = pd.read_sql_query(query, connection)

        # Save the DataFrame as a CSV file
        output_file = os.path.join(extraction_dir, f'{table_name}.csv')
        df.to_csv(output_file, index=False)

        # Add the table name to the list of stored tables
        stored_tables.append(output_file)

        print(f"Data extracted from table '{table_name}' and saved to {output_file}")

    # Close the connection to the Postgres database
    connection.close()
    return stored_tables


def extract_from_csv(date=get_date()):
    # Define the path to the CSV files to be extracted
    csv_paths = ['data/order_details.csv']
    
    # Define a list to store the names of the tables to be extracted
    stored_tables = []

    for csv_file_path in csv_paths:
        # Check if the CSV file exists
        if not os.path.isfile(csv_file_path):
            print(f"CSV file does not exist at: {csv_file_path}")
            continue

        # Read data from the CSV file into a Pandas DataFrame
        df = pd.read_csv(csv_file_path)

        # Define the directory to store the extracted data
        extraction_dir = f'data/csv/{date}'

        # Remove the directory if it exists, then create it
        if os.path.exists(extraction_dir):
            shutil.rmtree(extraction_dir)
        os.makedirs(extraction_dir)

        # Save the DataFrame as a CSV file
        output_file = os.path.join(extraction_dir, os.path.basename(csv_file_path))
        df.to_csv(output_file, index=False)

        # Add the table name to the list of stored tables
        stored_tables.append(output_file)

        print(f"Data extracted from CSV file '{csv_file_path}' and saved to {output_file}")

    return stored_tables


def load_into_final_database(csv_files, date=get_date()):
    # Define the SQLAlchemy engine

    # Attempt to establish a connection to the target PostgreSQL database
    try:
        connection = psycopg2.connect(**db_params)
        cursor = connection.cursor()

    except Exception as e:
        print(f"Error connecting to the database: {str(e)}")
        return

    try:
        # Disable autocommit mode to create a database
        connection.autocommit = True

        # Create a new database if it doesn't exist
        cursor.execute(f"CREATE DATABASE {config('OUTPUT_DB_NAME')}")

        # Re-enable autocommit mode for subsequent operations
        connection.autocommit = False

        connection.commit()
        print(f"Connected to database '{config('OUTPUT_DB_NAME')}'.")

    except psycopg2.Error as e:
        print(f"Didn't create database: {e}")

    finally:
        cursor.close()
        connection.close()

    # Iterate over the list of CSV file paths and load them into PostgreSQL
    for csv_file_path in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]

        try:

            # Read CSV file into a pandas DataFrame
            df = pd.read_csv(csv_file_path)

            # Drop old table and create new empty table
            df.head(0).to_sql(table_name, engine, if_exists='replace',index=False)

            for chunk in pd.read_csv(csv_file_path, encoding="utf-8", delimiter=',', chunksize=1000):
                connection = engine.raw_connection()
                cursor = connection.cursor()
                output = io.StringIO()
                chunk.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)
                contents = output.getvalue()
                cursor.copy_from(output, table_name, null="") # null values become ''
                connection.commit()
                cursor.close()
                connection.close()

            print(f"Data loaded into table '{table_name}' in the target database.")

        except Exception as e:
            print(f"Error loading data into table '{table_name}': {str(e)}")
            connection.rollback()
        finally:
            cursor.close()
    connection.close()


def execute_query(query, is_ddl=False):
    try:
        # Create a database connection
        connection = engine.raw_connection()
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(query)
        
        # Commit the changes for DDL queries
        if is_ddl:
            connection.commit()

        # Fetch the result as a DataFrame for SELECT queries
        if not is_ddl:
            query_result = pd.read_sql_query(query, connection)
        else:
            query_result = None
        
        # Close the cursor and connection
        cursor.close()
        connection.close()

        return query_result
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 1: Data Extraction and Local Storage
dag1 = DAG(
    'data_extraction_and_local_storage',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Daily execution
    catchup=False,  # Do not backfill past dates
)

# Create PythonOperator tasks for extraction and local storage
extract_postgres_task = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    dag=dag1,
)

extract_csv_task = PythonOperator(
    task_id='extract_from_csv',
    python_callable=extract_from_csv,
    dag=dag1,
)

# DAG 2: Data Loading to Final Database
dag2 = DAG(
    'data_loading_to_final_database',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Daily execution
    catchup=False,
)

# Define a sensor task in dag2 to wait for a task in dag1 to complete
wait_for_extract_task = ExternalTaskSensor(
    task_id='wait_for_extract_task',
    external_dag_id='data_extraction_and_local_storage',  # DAG ID of dag1
    external_task_id='extract_from_postgres',  # Task ID of the task to wait for in dag1
    dag=dag2,
)

# Create PythonOperator task for data loading
load_database_task = PythonOperator(
    task_id='load_into_final_database',
    python_callable=load_into_final_database,
    # op_args=[csv_files],  # Pass the list of CSV files as an argument
    dag=dag2,
)

# Task dependencies
wait_for_extract_task >> load_database_task
