
#& Code to take the Gravmax "CrusherPlant" Excel file, 
#& convert it to a dataframe, 
#& clean it up and then add it to SQL database,
#& and running to throught the column mapping 
#& from the column_mapping SQL table.

# Import Function Components
from components.excel_handling import drop_columns, forward_fill, read_excel_file, drop_redundant_datetime_column, set_higher_level_labels, create_groupnames_column
from components.data_transformation import concatenate_values, drop_unnecessary_rows, set_values_based_on_rows, process_ordered_set, handle_datetime_rows, assign_new_header, replace_redundant_values, rename_and_format_columns, concatenate_df_values
from components.database_handling import connect_to_sql, sql_column_mapping, check_and_create_table, upsert_data_to_sql

#& FUNCTION TO CONVERT THE EXCEL -> DATAFRAME -> SQL
def excel_to_sql_gravmax(file_path, db_name, table_name, server_name, username, password):
    
    #& READING AND HANDLING THE EXCEL FILE

    # Read the Excel file into a pandas DataFrame
    # components/excel_handling.py
    df = read_excel_file(file_path)

    #! TEMPORARY, SPEAK TO CLIENT
    # Set higher level labels for certain column ranges
    # components/excel_handling.py
    df = set_higher_level_labels(df)

    # Function to drop columns from 'Time on' to 'Running'
    # components/excel_handling.py
    df = drop_columns(df)

    # Forward fill to handle NaN values in row 1 and 2
    # components/excel_handling.py
    df = forward_fill(df)

    # Drop the redundant date and time column
    # components/excel_handling.py
    df = drop_redundant_datetime_column(df)

    # Insert a new column at location 1 for Group Names
    # components/excel_handling.py
    df = create_groupnames_column(df)

    #& DATA TRANSFORMATION

    # Loop through columns and concatenate values from row 2 and 3
    # components/data_transformation.py
    df = concatenate_values(df)

    # Drop unnecessary rows
    # components/data_transformation.py
    df = drop_unnecessary_rows(df)

    # Filling the GroupNames values based on the row
    # components/data_transformation.py
    df = set_values_based_on_rows(df)
    ordered_set = process_ordered_set(df)
    
    # Handling the datetime rows
    # components/data_transformation.py
    df = handle_datetime_rows(df)

    # Check if column header = Group name, if not, replace with empty string
    # components/data_transformation.py
    df = replace_redundant_values(df)

    # Make row 1 the column header
    # components/data_transformation.py
    df = assign_new_header(df)

    # Rename and Format columns
    # components/data_transformation.py
    df = rename_and_format_columns(df)

    # Combine columns with the same names ie OIL TEMPERATURE C IN 
    # components/data_transformation.py
    df = concatenate_df_values(df)

    #& DATABASE HANDLING
    
    # Connect to the SQL database
    # components/database_handling.py
    conn, cursor = connect_to_sql(server_name, db_name, username, password)

    # SQL mapping of columns
    # components/database_handling.py
    #TODO Add send email if columns mismatch
    #~ df = sql_column_mapping(df, cursor)

    # Check and create a table if no table exists
    # components/database_handling.py
    check_and_create_table(cursor, table_name, df)

    # Merge the Dataframe to the SQL Table
    # components/database_handling.py
    upsert_data_to_sql(conn, cursor, table_name, df, file_path, db_name)

