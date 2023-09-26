# handler.py
from watchdog.events import FileSystemEventHandler
from time import sleep
from TypeToSQL.csv_to_sql import csv_to_sql
from TypeToSQL.excel_to_sql import excel_to_sql
from TypeToSQL.json_to_sql import json_to_sql
from TypeToSQL.txt_to_sql import txt_to_sql
from TypeToSQL.db_to_sql import db_to_sql
from TypeToSQL.xml_to_sql import xml_to_sql
from TypeToSQL.gravmaxExcel import excel_to_sql_gravmax

# from googleapiclient.discovery import build
# from googleapiclient.errors import HttpError
# from google.oauth2.credentials import Credentials
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# import base64

class Handler(FileSystemEventHandler):
    def __init__(self, db_name, table_name, server_name, username, password):
        self.db_name = db_name
        self.table_name = table_name
        self.server_name = server_name
        self.username = username
        self.password = password

    # def send_email(self, subject, body):
    #     creds = Credentials.from_authorized_user_file('token.json')
    #     service = build('gmail', 'v1', credentials=creds)

    #     message = MIMEMultipart()
    #     message['to'] = 'christian@intellicode.co.za'
    #     message['subject'] = subject
    #     message.attach(MIMEText(body, 'plain'))

    #     raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    #     message = service.users().messages().send(userId='me', body={'raw': raw_message})

    #     try:
    #         message.execute()
    #         print('Message sent')
    #     except HttpError as error:
    #         print(f'An error occurred: {error}')

    def on_created(self, event):
        file_path = event.src_path
        if file_path.endswith(".csv"):
            print(f"New CSV file {file_path} has been created!")
            sleep(1)
            success = csv_to_sql(file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        elif file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            print(f"New Excel file {file_path} has been created!")
            sleep(1)
            success = excel_to_sql_gravmax(file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        elif file_path.endswith(".txt"):
            print(f"New Text file {file_path} has been created!")
            sleep(1)
            success = txt_to_sql(file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        elif file_path.endswith(".json"):
            print(f"New JSON file {file_path} has been created!")
            sleep(1)
            success = json_to_sql(file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        elif file_path.endswith(".db"):
            print(f"New DB file {file_path} has been created!")
            sleep(1)
            success = db_to_sql(file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        elif file_path.endswith(".xml"):
            print(f"New XML file {file_path} has been created!")
            sleep(1)
            success = xml_to_sql(file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        else:
            print(f"Unsupported file type: {file_path}")
            return

        # if success:
        #     self.send_email('File Processed', f'File {file_path} has been successfully processed.')

