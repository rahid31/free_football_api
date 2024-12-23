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
    # If the nested_json is None or not a dictionary, return an empty list
    if nested_json is None:
        return items

    # Only proceed if nested_json is a dictionary
    if isinstance(nested_json, dict):
        for key, value in nested_json.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, dict):
                items.extend(flatten_json(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                for i, sub_value in enumerate(value):
                    if isinstance(sub_value, dict):
                        items.extend(flatten_json(sub_value, f"{new_key}_{i}", sep=sep).items())
                    else:
                        items.append((f"{new_key}_{i}", sub_value))
            else:
                items.append((new_key, value))
    
    # If the nested_json is a list, flatten each element
    elif isinstance(nested_json, list):
        for i, sub_value in enumerate(nested_json):
            new_key = f"{parent_key}_{i}" if parent_key else str(i)
            if isinstance(sub_value, dict):
                items.extend(flatten_json(sub_value, new_key, sep=sep).items())
            else:
                items.append((new_key, sub_value))
    
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

# Truncate the table if already exists
def truncate_table(connection, table_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")
        connection.commit()
        print(f"Table `{table_name}` truncated successfully.")
    except mysql.connector.Error as err:
        print(f"Error truncating table {table_name}: {err}")
    finally:
        cursor.close()

# Fetch data for the "main_league" function
def main_league(url, params=None):
    response = requests.get(url, headers=headers, params=params if params else None)
    
    if response.status_code == 200:
        data = response.json().get('response', {}).get('leagues', [])
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

# Fetch data for the "transfer_data" function    
def transfer_data(url, params):
    data = []
    response = requests.get(url, headers=headers, params=params if params else None)
    
    if response.status_code == 200:
        total_row_count = response.json().get('response', {}).get('hits', [])
        limit = 100
        total_pages = (total_row_count + limit - 1) // limit
        print(f"Total rows: {total_row_count}")
        print(f"Total pages available: {total_pages}")

        for page in range(1, total_pages + 1):
            params['page'] = page 
                
            response = requests.get(url, headers=headers, params=params if params else None)
            
            if response.status_code == 200:
                data_page = response.json().get('response', {}).get('transfers', [])
                if data_page:
                    data.extend(data_page)
                    print(f"Page {page} fetched successfully.")
                else:
                    print(f"No data found on page {page}.")
            else:
                print(f"Error on page {page}: {response.status_code}, {response.text}")
    else:
        print(f"Failed to fetch data: {response.status_code}, {response.text}")
    
    return data

# Function to execute the API request and insert data into MySQL
def execute_request(function_name, url, params, table_name):

    data = function_name(url, params)
    
    if not data:
        return

    # Flatten data and insert into MySQL
    try:
        connection = connect_to_mysql()

        # Check if table exists and truncate if it does
        if table_exists(connection, table_name):
            truncate_table(connection, table_name)
        
        # Flatten the JSON data
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
        "url": "https://free-api-live-football-data.p.rapidapi.com/football-get-all-leagues",
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
    },
    "transfer_data": {
        "url": "https://free-api-live-football-data.p.rapidapi.com/football-get-all-transfers",
        "params": {
            "page": "1",
            "fields": "name, playerId, position_label, position_key, transferDate"
        },
        "table_name": "transfer_data"
    }
}

# Main function to execute different API requests
def main():
    execute_request(main_league, urls["main_league"]["url"], urls["main_league"]["params"], urls["main_league"]["table_name"])
    execute_request(main_country, urls["main_country"]["url"], urls["main_country"]["params"], urls["main_country"]["table_name"])
    execute_request(epl_matches, urls["epl_matches"]["url"], urls["epl_matches"]["params"], urls["epl_matches"]["table_name"])
    execute_request(transfer_data, urls["transfer_data"]["url"], urls["transfer_data"]["params"], urls["transfer_data"]["table_name"])

if __name__ == "__main__":
    main()