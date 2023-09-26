# handler.py
from watchdog.events import FileSystemEventHandler
from time import sleep
from crusher_plant import crusher_plant_excel_to_sql
from spreadsheet_splitter.sheet_splitter import split_excel_sheets
from components.send_email import send_email


class Handler(FileSystemEventHandler):
    def __init__(self, db_name, table_name, server_name, username, password):
        self.db_name = db_name
        self.table_name = table_name
        self.server_name = server_name
        self.username = username
        self.password = password

    def on_created(self, event):
        file_path = event.src_path

        if file_path.endswith(".xlsx") or file_path.endswith(".xls"):
            print(f"New Excel file {file_path} has been created!")
            sleep(1)
            success = crusher_plant_excel_to_sql(
                file_path, self.db_name, self.table_name, self.server_name, self.username, self.password)
        else:
            print(f"Unsupported file type: {file_path}")
            return

        if success:
            self.send_email(
                'File Processed', f'File {file_path} has been successfully processed.')
