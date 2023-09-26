# File to SQL Program

This program allows you to automatically process different file types and write their data to a SQL Server database. It supports CSV, Excel, TXT, JSON, XML, and SQLite database files.

## Prerequisites

Before using this program, make sure you have the following dependencies installed:

- Python 3
- pandas library (`pip install pandas`)
- pyodbc library (`pip install pyodbc`)
- sqlite3 library (included with Python)
- watchdog library (`pip install watchdog`)
- google-api-python-client library (`pip install google-api-python-client`)
- google-auth library (`pip install google-auth`)
- google-auth-oauthlib library (`pip install google-auth-oauthlib`)
- google-auth-httplib2 library (`pip install google-auth-httplib2`)


## Usage

1. Clone or download the program files from the repository.

2. Install the required dependencies by running the following command in your terminal or command prompt:

```
pip install pandas pyodbc
```

3. Configure the program settings in the `handler.py` file:
   - Set the `db_name` variable to the name of your SQL Server database.
   - Set the `table_name` variable to the name of the table where you want to write the data.
   - Set the `server_name` variable to the name of your SQL Server instance.

---
    Remember to make sure the directory path in `watcher.py` is set to the correct path and pointing at the correct directory
---

4. Run the program by executing the `main.py` file:
```
python main.py
```

5. The program will start monitoring the specified directory for new file creations. When a new file is detected, it will be processed according to its file type:
   - CSV files will be read and their data will be written to the SQL Server database.
   - Excel files (XLSX or XLS) will be read and their data will be written to the SQL Server database.
   - TXT files will be read and their data will be written to the SQL Server database.
   - JSON files will be read and their data will be written to the SQL Server database.
   - XML files will be read and their data will be written to the SQL Server database.
   - SQLite database files will be read and their data will be written to the SQL Server database.

6. The program will send an email notification when a file has been successfully processed.

7. To stop the program, press `Ctrl + C` in the terminal or command prompt.

---

## Troubleshooting

- If you encounter any errors related to missing dependencies, make sure you have installed the required libraries mentioned in the "Prerequisites" section.

- If you encounter any issues with the SQL Server connection, double-check the `db_name`, `table_name`, and `server_name` variables in the `handler.py` file. Ensure that they are correctly set to match your SQL Server configuration.

- If you encounter any issues with file processing or data insertion, check the file format and structure to ensure it matches the expected format for each supported file type.

- If you encounter any other errors or issues, please refer to the error messages and traceback information provided. This can help identify the specific problem and guide you in troubleshooting and resolving the issue.

### Conclusion

The File to SQL program provides a convenient way to automatically process various file types and write their data to a SQL Server database. By following the instructions in this `README.md` file, you can easily set up and use the program to streamline your data processing workflow. 