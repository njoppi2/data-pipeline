from typing import Any, List, Optional, Union
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.models.param import Param
from datetime import datetime, timedelta
import os
import pandas as pd
from decouple import config
import psycopg2
from psycopg2 import sql
import shutil
import csv
import io
from sqlalchemy import create_engine
import glob

# Define constants
POSTGRES_DIR = 'postgres'
CSV_DIR = 'csv'
QUERY_DIR = 'query'
DELIMITER = ','

# Define the parameters for the source database
db_params = {
    'dbname': config('DB_NAME'),
    'user': config('DB_USER'),
    'password': config('DB_PASSWORD'),
    'host': config('DB_HOST'),
    'port': config('DB_PORT'),
}

# Create an engine to the target database
engine = create_engine(f'postgresql://{config("DB_USER")}:{config("DB_PASSWORD")}@{config("DB_HOST")}:{config("DB_PORT")}/{config("OUTPUT_DB_NAME")}')

# Define data directory
current_dir = os.path.dirname(os.path.abspath(__file__))
base_dir = config('DATA_DIR') or os.path.abspath(f'{current_dir}/../data')

def get_date() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def create_csv(df: pd.DataFrame, base_path: str, file_name: str) -> str:
    # Remove the directory if it exists, then create it
    if os.path.exists(base_path):
        shutil.rmtree(base_path)
    os.makedirs(base_path)

    # Save the DataFrame as a CSV file
    output_file = os.path.join(base_path, file_name)
    df.to_csv(output_file, index=False)
    return output_file


def extract_from_postgres(**raw_context: Any) -> List[str]:
    context = {"params": {"date": None}}
    context.update(raw_context)

    # Define the date to be used for the extraction
    date = context["params"]["date"] or get_date()

    # Establish a connection to the Postgres database
    with psycopg2.connect(**db_params) as connection:
        # Get a list of all tables in the database
        with connection.cursor() as cursor:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
            tables = cursor.fetchall()

        # Define a list to store the names of the tables to be extracted
        stored_tables = []

        # Create a directory to store the CSV files for this extraction
        for raw_table_name in tables:
            table_name = raw_table_name[0]
            postgres_path = f'{base_dir}/{POSTGRES_DIR}/{table_name}/{date}'

            # Define the SQL query to extract all data from the current table
            query = f"SELECT * FROM {table_name};"

            # Save data in CSV file
            df = pd.read_sql_query(query, connection)
            file_name = f'{table_name}.csv'
            output_file = create_csv(df, postgres_path, file_name)
            
            # Add the table name to the list of stored tables
            stored_tables.append(output_file)

            print(f"Data extracted from table '{table_name}' and saved to {output_file}")

    return stored_tables


def extract_from_csv(**raw_context: Any) -> List[str]:
    context = {"params": {"date": None}}
    context.update(raw_context)

    # Define the date to be used for the extraction
    date = context["params"]["date"] or get_date()

    # Define the path to the CSV files to be extracted
    input_csv_paths = [f'{base_dir}/order_details.csv']
    
    # Define a list to store the names of the tables to be extracted
    stored_tables = []

    for input_csv_path in input_csv_paths:
        csv_path = f'{base_dir}/{CSV_DIR}/{date}'

        # Check if the CSV file exists
        if not os.path.isfile(input_csv_path):
            print(f"CSV file does not exist at: {input_csv_path}")
            continue

        # Save data in CSV file
        df = pd.read_csv(input_csv_path)
        file_name = os.path.basename(input_csv_path)
        output_file = create_csv(df, csv_path, file_name)

        # Add the table name to the list of stored tables
        stored_tables.append(output_file)

        print(f"Data extracted from CSV file '{input_csv_path}' and saved to {output_file}")

    return stored_tables


def load_into_final_database(**raw_context: Any) -> None:
    context = {"params": {"date": None}}
    context.update(raw_context)

    # Define the date to be used for the extraction
    date = context["params"]["date"] or get_date()
    csv_files = context["params"].get("csv_files", None)

    if csv_files is None:
        # Define the list of CSV files to be loaded into the database
        csv_files = glob.glob(os.path.join(base_dir, CSV_DIR, date, '*.csv'))
        postgres_files = glob.glob(os.path.join(base_dir, POSTGRES_DIR, '**', date, '*.csv'), recursive=True)        
        # Combine both lists of files
        csv_files.extend(postgres_files)
    
    # Make sure step 2 can't be executed if step 1 hasn't been successfully completed
    missing_files = [file_path for file_path in csv_files if not os.path.isfile(file_path)]
    if missing_files or not csv_files:
        missing_files_str = "\n".join(missing_files)
        raise Exception(f"CSV files do not exist at:\n{missing_files_str}" if missing_files else "No CSV files to load into the database.")

    # Create a new database if it doesn't exist
    try:
        connection = psycopg2.connect(**db_params)
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {config('OUTPUT_DB_NAME')}")
            connection.commit()
        print(f"Connected to database '{config('OUTPUT_DB_NAME')}'.")
    except psycopg2.errors.DuplicateDatabase as e:
        print(f"{str(e)}")
    except Exception as e:
        raise Exception(f"An error occurred: {str(e)}")
    finally:
        connection.close()

    # Iterate over the list of CSV file paths and load them into PostgreSQL
    for csv_file_path in csv_files:
        table_name = os.path.splitext(os.path.basename(csv_file_path))[0]

        try:
            df = pd.read_csv(csv_file_path)
            df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)
        
            with engine.begin() as connection:
                with connection.connection.cursor() as cursor:
                    for chunk in pd.read_csv(csv_file_path, delimiter=DELIMITER, chunksize=1000):
                        output = io.StringIO()
                        chunk.to_csv(output, sep="\t", header=False, index=False)
                        output.seek(0)
                        cursor.copy_from(output, table_name, null="")
        
            print(f"Data loaded into table '{table_name}' in the target database.")
        except Exception as e:
            raise Exception(f"Error loading data into table '{table_name}': {str(e)}")


def execute_query(query: str, is_ddl: bool = False) -> Optional[pd.DataFrame]:
    # Create a database connection
    with engine.begin() as connection:
        with connection.connection.cursor() as cursor:
            # Execute the query
            cursor.execute(query)

            # Fetch the result as a DataFrame for SELECT queries
            if not is_ddl:
                query_result = pd.read_sql_query(query, connection)
                query_path = f'{base_dir}/{QUERY_DIR}'
                os.makedirs(query_path, exist_ok=True)
                query_result.to_csv(f'{query_path}/result.csv', index=False)
            else:
                query_result = None
                connection.commit()  # Commit the changes for DDL queries

    return query_result


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 8, 19),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
    'schedule_interval': timedelta(days=1),
    'params': {
        "date": Param(
            get_date(),
            type="string",
            format="date",
        ),
    },
}

# DAG 1: Data Extraction and Local Storage
dag1 = DAG(
    'data_extraction_and_local_storage',
    default_args=default_args,
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
)

# Define a sensor task in dag2 to wait for a task in dag1 to complete
# wait_for_extract_task = ExternalTaskSensor(
#     task_id='wait_for_extract_task',
#     external_dag_id='data_extraction_and_local_storage',  # DAG ID of dag1
#     external_task_id='extract_from_postgres',  # Task ID of the task to wait for in dag1
#     dag=dag2,
# )


# Create PythonOperator task for data loading
load_database_task = PythonOperator(
    task_id='load_into_final_database',
    python_callable=load_into_final_database,
    # op_args=[csv_files],
    dag=dag2,
)