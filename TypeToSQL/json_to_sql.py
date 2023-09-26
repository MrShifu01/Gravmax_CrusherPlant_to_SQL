import pandas as pd
import pyodbc
import json
from datetime import datetime, timedelta

def json_to_sql(file_path, db_name, table_name, server_name, username, password):
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Could not read JSON file {file_path}: {e}")
        return

    # Define the nested fields to be converted to datetime
    nested_fields = [
        "clockIn", "clockOut", "lunchTime", 
        "normalHours", "overtimeHours", "sundayOvertime", "holidayTime"
    ]

    # Convert nested dictionaries to datetime
    for record in data:
        # Convert date field to datetime object
        base_date = datetime.fromisoformat(record["date"].replace("T", " "))
        
        for field in nested_fields:
            if field in record and isinstance(record[field], dict):
                hours = record[field]["hours"]
                minutes = record[field]["minutes"]
                seconds = record[field]["seconds"]
                milliseconds = record[field]["milliseconds"]
                # Construct the full datetime
                full_datetime = base_date.replace(hour=hours, minute=minutes, second=seconds, microsecond=milliseconds*1000)
                # Convert the full datetime to the desired string format
                record[field] = full_datetime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        # Convert boolean values to integers
        for key, value in record.items():
            if isinstance(value, bool):
                record[key] = int(value)

    # Create a main dataframe
    main_df = pd.DataFrame(data)

    # Connect to the SQL server and create the database if it doesn't exist
    conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE=master;UID={username};PWD={password}'
    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'{db_name}') CREATE DATABASE {db_name}")
        conn.close()
    except Exception as e:
        print(f"Could not create or connect to database: {e}")
        return

    # Connect to the created database
    conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db_name};UID={username};PWD={password}'
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Could not connect to database: {e}")
        return

    # Create the main table and nested tables with appropriate relationships
    try:
        # Create the main table
        main_cols = ", ".join([f"[{col}] NVARCHAR(MAX)" for col in main_df.columns])
        cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ([ID] INT IDENTITY(1,1) PRIMARY KEY, {main_cols})")

    except Exception as e:
        print(f"Could not create table: {e}")
        return

    # Insert data into the main table and nested tables
    try:
        for index, row in main_df.iterrows():
            # Insert data into the main table and retrieve the primary key
            values = ", ".join([f"'{value}'" for value in row.astype(str).values])
            cursor.execute(f"INSERT INTO [{table_name}] ({', '.join(main_df.columns)}) OUTPUT INSERTED.ID VALUES ({values})")
            pk = cursor.fetchone()[0]
        
        conn.commit()
    except Exception as e:
        print(f"Could not perform UPSERT operation: {e}")
        return

    print(f"Data from {file_path} has been written to {db_name}.{table_name}")
    return True
