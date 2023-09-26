import pandas as pd
import xml.etree.ElementTree as ET
import pyodbc

def xml_to_sql(file_path, db_name, table_name, server_name, username, password):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        data = []
        for search_info in root.iter('SearchInfo'):
            record = {}
            for child in search_info:
                record[child.tag] = child.text
            data.append(record)

        df = pd.DataFrame(data)
    except Exception as e:
        print(f"Could not read XML file {file_path}: {e}")
        return None

    # Step 2: Remove whitespace from strings
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

    # Shorten column names to fit SQL Server's limit
    df.columns = [col[:128] for col in df.columns]

    conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE=master;UID={username};PWD={password}'

    try:
        conn = pyodbc.connect(conn_str, autocommit=True)
        cursor = conn.cursor()
        cursor.execute(f"IF NOT EXISTS (SELECT name FROM master.dbo.sysdatabases WHERE name = N'{db_name}') CREATE DATABASE {db_name}")
        conn.close()
    except Exception as e:
        print(f"Could not create or connect to database: {e}")
        return

    conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db_name};UID={username};PWD={password}'

    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Could not connect to database: {e}")
        return

    try:
        cols = ", ".join([f"[{col}] NVARCHAR(MAX)" if col != 'PrimaryKey' else "[PrimaryKey] INT" for col in df.columns])
        cursor.execute(f"""IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ([PrimaryKey] INT IDENTITY(1,1) PRIMARY KEY, {cols})""")
    except Exception as e:
        print(f"Could not create table: {e}")
        return

    # Prepare data and query for the MERGE operation
    try:
        # Get the list of columns excluding 'PrimaryKey'
        columns = [col for col in df.columns if col != 'PrimaryKey']
        columns_bracketed = [f"[{col}]" for col in columns]

        # Generate ON clause using all columns (other than 'PrimaryKey')
        on_conditions = " AND ".join([f"Target.{col} = Source.{col}" for col in columns_bracketed])

        for index, row in df.iterrows():
            # Get the values excluding 'PrimaryKey' column
            values = ", ".join([f"'{value}'" for col, value in zip(df.columns, row.astype(str).values) if col != 'PrimaryKey'])

            # Construct the merge query using the formatted values and conditions
            merge_query = f"""
                MERGE INTO [{table_name}] AS Target
                USING (VALUES ({values})) AS Source ({', '.join(columns_bracketed)})
                ON ({on_conditions})
                WHEN MATCHED THEN 
                    UPDATE SET {', '.join([f'Target.{col} = Source.{col}' for col in columns_bracketed])}
                WHEN NOT MATCHED BY TARGET THEN
                    INSERT ({', '.join(columns_bracketed)})
                    VALUES ({', '.join([f'Source.{col}' for col in columns_bracketed])});
                """
            
            cursor.execute(merge_query)
        conn.commit()
    except Exception as e:
        print(f"Could not perform UPSERT operation: {e}")
        return

    print(f"Data from {file_path} has been written to {db_name}.{table_name}")
    return True
