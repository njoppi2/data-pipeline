import psycopg2
import os
from datetime import datetime
from decouple import config


def check_postgres_health():
    try:
        # Establish a connection to the Postgres database
        db_params = {
            'dbname': config('POSTGRES_DB'),
            'user': config('POSTGRES_USER'),
            'password': config('POSTGRES_PASSWORD'),
        }
        connection = psycopg2.connect(**db_params)

        # Define a simple query to check database availability (you can modify this)
        query = "SELECT 1"

        # Create a cursor and execute the query
        cursor = connection.cursor()
        cursor.execute(query)

        # Check if the query was successful
        if cursor.fetchone()[0] == 1:
            print("PostgreSQL is healthy.")
        else:
            print("PostgreSQL is not healthy.")

        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

check_postgres_health()