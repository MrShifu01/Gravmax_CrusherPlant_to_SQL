# Code to take the Gravmax "CrusherPlant" Excel file, convert it to a dataframe, clean it up and then add it to SQL database and running ti throught the column mapping from the column_mapping SQL table.

# Import pandas for the dataframe
# Import pyodbc to be able to connect to SQL Server
import pandas as pd
import pyodbc

#* FUNCTION TO CONVERT THE EXCEL -> DATAFRAME -> SQL
def excel_to_sql_gravmax(file_path, db_name, table_name, server_name, username, password):
    
    # Read the Excel file into a pandas DataFrame
    try:
        df = pd.read_excel(file_path, header=None)
    except Exception as e:
        print(f"Could not read Excel file {file_path}: {e}")
        return

    #* TRANSFORM EXCEL INTO VALID DATAFRAME

    # Set higher level labels for certain column ranges
    #! TEMPORARY, SPEAK TO CLIENT
    df.loc[0, 6:13] = 'Secondary Crusher'
    df.loc[0, 13:20] = 'Tertiary Crusher'


    # Indexing columns between "Time on" and "Running"
    for idx, col in enumerate(df.columns):
        if df.at[1, col] == "Time on":
            start_index = idx
        elif df.at[1, col] == "Running":
            end_index = idx
            break

    # Add columns between 'Time on' and 'Running' to cols_to_drop
    if start_index is not None and end_index is not None:
        cols_to_drop = df.columns[start_index + 1:end_index].to_list()  # This should get the column names
        df = df.drop(columns=cols_to_drop)

    # Drop the columns
    df = df.drop(columns=cols_to_drop)

    # Forward fill to handle NaN values in row 0 and 1
    df.iloc[0] = df.iloc[0].fillna(method='ffill')
    df.iloc[1] = df.iloc[1].fillna(method='ffill')

    df = df.drop(df.columns[1], axis=1)  # Drop the redundant date and time column

    # Loop through columns and concatenate values 
    for col in df.columns:
        val_1 = df.at[1, col]
        val_2 = df.at[2, col]
        
        if isinstance(val_1, str) and isinstance(val_2, str):
            df.at[1, col] = val_1 + ' ' + val_2

    df.insert(loc=1, column='GroupNames', value='')

    df = df.drop([2, 3, 4, 5])  # Drop unnecessary rows

    # Find the rows to drop based on conditions
    rows_to_drop = df.index[2:][df.iloc[2:, 0].isna()]

    # Drop those rows
    df.drop(rows_to_drop, inplace=True)

    # Getting the name of the column at index 2
    col_name = df.columns[2]

    row_1_value = df.loc[0, col_name]
    row_2_value = df.loc[1, col_name]

    # Filling the 'GroupNames' column with the value from row_1_value, from row 3 onwards
    df.iloc[2:, df.columns.get_loc('GroupNames')] = row_1_value
    df = df.reset_index(drop=True)

    from collections import OrderedDict

    # Loop through values of row 1
    row_1_values = df.loc[0].tolist()

    # Create an ordered set from the list
    ordered_set = list(OrderedDict.fromkeys(row_1_values))

    # Filter ordered_set to keep only string items
    ordered_set = [item for item in ordered_set if isinstance(item, str)]

    # Extract datetime rows
    datetime_rows = df[df[0].apply(lambda x: isinstance(x, pd.Timestamp))].copy()

    # A new DataFrame to store the result
    new_df = df.copy()

    # For each group in the ordered_set, copy the datetime rows and set the GroupNames column
    for group_name in ordered_set[2:]:
        temp_df = datetime_rows.copy()
        temp_df['GroupNames'] = group_name
        new_df = new_df.append(temp_df, ignore_index=True)

    # Update the main DataFrame
    df = new_df

    # Loop through columns starting from index 2
    for col in df.columns[2:]:
        # Get the value of the first row in the current column
        first_row_value = df.loc[0, col]

        # Loop through rows starting from index 2
        for index, row in df[2:].iterrows():
            # Check if the first row value doesn't match the 'GroupNames' value
            if first_row_value != row['GroupNames']:
                # Replace the value in the current column with an empty string
                df.loc[index, col] = ''

    # Drop row 0
    df = df.drop(0)

    # Make row 1 the column header
    df.columns = df.iloc[0]

    # Drop the now redundant row 1
    df = df.drop(1)

    # Reset the index
    df = df.reset_index(drop=True)

    # Helper function to rename columns based on certain conditions
    def rename_columns(df, substr, suffixes):
        seen = {}
        new_columns = []

        for col in df.columns:
            if isinstance(col, str) and substr in col:
                if col not in seen:
                    seen[col] = 1
                else:
                    seen[col] += 1

                new_columns.append(col + suffixes[min(seen[col] - 1, len(suffixes) - 1)])
            else:
                new_columns.append(col)

        df.columns = new_columns

    df.columns.values[0] = 'DateTime'
    df['DateTime'] = df['DateTime'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df.columns.values[1] = 'GroupName'

    # Replace next line with empty string
    df.columns = df.columns.str.replace('\n', ' ')

    # Remove whitespace from column names
    df.columns = df.columns.str.strip()

    # Remove non alpha and numeric characters from coulmn names
    df.columns = df.columns.str.replace(r'[^a-zA-Z0-9]', '_', regex=True)

    def concatenate_values(x):
        return x.apply(lambda y: ''.join(y.dropna().astype(str)), axis=1)

    # Create an empty DataFrame to store results
    result_df = pd.DataFrame(index=df.index)

    # Iterate over unique column names while preserving order
    for col_name in df.columns.unique():
        # Filter original DataFrame for columns with the same name
        sub_df = df.loc[:, df.columns == col_name]

        # Apply the concatenate function and store the result
        result_df[col_name] = concatenate_values(sub_df)

    df = result_df

    ################## Connect to the SQL Server and write data to the database ##################
    
    conn_str = f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db_name};UID={username};PWD={password}'
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Could not connect to database: {e}")
        return

    ####################### SQL MAPPING OF COLUMNS ##########################

    # cursor.execute("SELECT old_name, new_name FROM column_mapping")
    # mappings = cursor.fetchall()
    # column_mapping = {old: new for old, new in mappings}

    # # Compare column names
    # df_columns = set(df.columns)
    # mapping_old_columns = set(column_mapping.keys())

    # mismatches = df_columns - mapping_old_columns

    # if mismatches:
    #     # Raise an alert or handle as needed
    #     alert_message = f"Column name mismatch detected! The following columns in the DataFrame don't match the SQL mapping: {', '.join(mismatches)}"
    #     # Here you could potentially send an email, raise an exception, print to a log file, etc.
    #     raise ValueError(alert_message)

    # # If no mismatches, can continue with renaming process
    # df.rename(columns=column_mapping, inplace=True)

    try:
        cols = ", ".join([f"[{col}] NVARCHAR(MAX)" if col != 'PrimaryKey' else "[PrimaryKey] INT" for col in df.columns])
        cursor.execute(f"""IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ([PrimaryKey] INT IDENTITY(1,1) PRIMARY KEY, {cols})""")

        # print(f"Table {table_name} created successfully with columns: {cols}")
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

