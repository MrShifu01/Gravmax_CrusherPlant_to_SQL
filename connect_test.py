import pyodbc

server = '102.221.36.221,9555'
database = 'CHRISTIAN_TEST'
username = 'christian'
password = 'c852456!'
driver = '{ODBC Driver 17 for SQL Server}'

try:
    cnxn = pyodbc.connect('DRIVER=' + driver + 
                      ';SERVER=' + server + 
                      ';DATABASE=' + database + 
                      ';UID=' + username + 
                      ';PWD=' + password)
    cursor = cnxn.cursor()
    print('Connection established')
except Exception as e:
    print(f'Cannot connect to SQL server: {e}')
