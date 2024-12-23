import os
import requests
import mysql.connector
import pandas as pd
from dotenv import load_dotenv
from collections.abc import Mapping

# Load environment variables
load_dotenv()

# API headers
headers = {
    "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
    "x-rapidapi-host": os.getenv("RAPIDAPI_HOST")
}

# MySQL credentials
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")

# Set up MySQL connection
def connect_to_mysql():
    connection = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )
    return connection

# Flatten nested JSON data
def flatten_json(nested_json, parent_key='', sep='_'):
    items = []
    for key, value in nested_json.items():
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, Mapping):
            items.extend(flatten_json(value, new_key, sep=sep).items())
        elif isinstance(value, list):
            for i, sub_value in enumerate(value):
                items.extend(flatten_json(sub_value, f"{new_key}_{i}", sep=sep).items())
        else:
            items.append((new_key, value))
    return dict(items)

# Insert data into MySQL table
def insert_json_to_mysql(flat_json_data, table_name, connection):
    cursor = connection.cursor()

    # Prepare the INSERT INTO SQL query
    columns = ", ".join(flat_json_data.keys())
    placeholders = ", ".join(["%s"] * len(flat_json_data))
    query = f"INSERT INTO `{table_name}` ({columns}) VALUES ({placeholders})"
    
    # Execute the insertion
    cursor.execute(query, list(flat_json_data.values()))
    connection.commit()
    cursor.close()
    print(f"Data inserted successfully into {table_name}")

# Check if table exists in the database
def table_exists(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
    result = cursor.fetchone()
    cursor.close()
    return result is not None

# Create table based on the flattened JSON data
def create_table_from_flat_json(connection, table_name, flat_json_data):
    cursor = connection.cursor()
    columns = []

    # Dynamically create the columns based on the data types
    for column_name, value in flat_json_data.items():
        if isinstance(value, int):
            sql_type = "INT"
        elif isinstance(value, float):
            sql_type = "FLOAT"
        elif isinstance(value, bool):
            sql_type = "BOOLEAN"
        elif isinstance(value, str):
            sql_type = "TEXT"
        else:
            sql_type = "TEXT"
        
        columns.append(f"`{column_name}` {sql_type}")
    
    # Create table SQL query
    columns_sql = ", ".join(columns)
    create_table_sql = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({columns_sql});"
    
    # Execute the table creation
    cursor.execute(create_table_sql)
    connection.commit()
    cursor.close()
    print(f"Table `{table_name}` is ready (created or already exists).")

# Fetch data for the "main_league" function
def main_league(url, params=None):
    response = requests.get(url, headers=headers, params=params if params else None)
    
    if response.status_code == 200:
        data = response.json().get('response', {}).get('popular', [])
        return data
    else:
        print(f"Error fetching data from {url}, Status Code: {response.status_code}")
        return []

# Fetch data for the "main_country" function
def main_country(url, params=None):
    response = requests.get(url, headers=headers, params=params if params else None)
    
    if response.status_code == 200:
        data = response.json().get('response', {}).get('countries', [])
        return data
    else:
        print(f"Error fetching data from {url}, Status Code: {response.status_code}")
        return []

# Fetch data for the "epl_matches" function
def epl_matches(url, params):
    response = requests.get(url, headers=headers, params=params if params else None)
    
    if response.status_code == 200:
        data = response.json().get('response', {}).get('matches', [])
        return data
    else:
        print(f"Error fetching data from {url}, Status Code: {response.status_code}")
        return []

# Function to execute the API request and insert data into MySQL
def execute_request(function_name, url, params, table_name):

    data = function_name(url, params)
    
    if not data:
        return

    # Flatten data and insert into MySQL
    try:
        connection = connect_to_mysql()

        for item in data:
            flat_item = flatten_json(item)
            
            # Check if the table exists and create it if it does not
            if not table_exists(connection, table_name):
                create_table_from_flat_json(connection, table_name, flat_item)
            
            # Insert the flattened data into the MySQL table
            insert_json_to_mysql(flat_item, table_name, connection)
        
    except mysql.connector.Error as e:
        print(f"Error: {e}")
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection closed.")

# API URLs and parameters
urls = {
    "main_league": {
        "url": "https://free-api-live-football-data.p.rapidapi.com/football-popular-leagues",
        "params": None,
        "table_name": "league_data"
    },
    "main_country": {
        "url": "https://free-api-live-football-data.p.rapidapi.com/football-get-all-countries",
        "params": None,
        "table_name": "country_data"
    },
    "epl_matches": {
        "url": "https://free-api-live-football-data.p.rapidapi.com/football-get-all-matches-by-league",
        "params": {
            "leagueid" : "47"
        },
        "table_name": "epl_data"
    }
}

# Main function to execute different API requests
def main():
    execute_request(main_league, urls["main_league"]["url"], urls["main_league"]["params"], urls["main_league"]["table_name"])
    execute_request(main_country, urls["main_country"]["url"], urls["main_country"]["params"], urls["main_country"]["table_name"])
    execute_request(epl_matches, urls["epl_matches"]["url"], urls["epl_matches"]["params"], urls["epl_matches"]["table_name"])

if __name__ == "__main__":
    main()