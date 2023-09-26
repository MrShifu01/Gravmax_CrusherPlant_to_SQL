import pyodbc

def connect_to_sql(server_name, db_name, username, password):
    conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db_name};UID={username};PWD={password}'
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Could not connect to database: {e}")
        return
    return conn, cursor

def sql_column_mapping(df, cursor):
    cursor.execute("SELECT old_name, new_name FROM column_mapping")
    mappings = cursor.fetchall()
    column_mapping = {old: new for old, new in mappings}

    # Compare column names
    df_columns = set(df.columns)
    mapping_old_columns = set(column_mapping.keys())

    mismatches = df_columns - mapping_old_columns

    if mismatches:
        # Raise an alert or handle as needed
        alert_message = f"Column name mismatch detected! The following columns in the DataFrame don't match the SQL mapping: {', '.join(mismatches)}"
        # Here you could potentially send an email, raise an exception, print to a log file, etc.
        raise ValueError(alert_message)

    # If no mismatches, can continue with renaming process
    df.rename(columns=column_mapping, inplace=True)

    return df

def check_and_create_table(cursor, table_name, df):
    try:
        cols = ", ".join([f"[{col}] NVARCHAR(MAX)" if col != 'PrimaryKey' else "[PrimaryKey] INT" for col in df.columns])
        cursor.execute(f"""IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ([PrimaryKey] INT IDENTITY(1,1) PRIMARY KEY, {cols})""")

    # print(f"Table {table_name} created successfully with columns: {cols}")
    except Exception as e:
        print(f"Could not create table: {e}")
        return
    
def upsert_data_to_sql(conn, cursor, table_name, df, file_path, db_name):
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