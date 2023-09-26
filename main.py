import pyodbc
from watcher import Watcher

server = '102.221.36.221,9555'
database = 'CHRISTIAN_TEST'
username = 'christian'
password = 'c852456!'
driver = '{ODBC Driver 17 for SQL Server}'

table_name = 'testingGravmax'

try:
    cnxn = pyodbc.connect('DRIVER=' + driver +
                          ';SERVER=' + server +
                          ';DATABASE=' + database +
                          ';UID=' + username +
                          ';PWD=' + password)
    cursor = cnxn.cursor()
    print('Connection established')

    # Create an instance of the Watcher class and run it
    watcher = Watcher(database, table_name, server, username, password)
    watcher.run()

except Exception as e:
    print(f'Cannot connect to SQL server: {e}')