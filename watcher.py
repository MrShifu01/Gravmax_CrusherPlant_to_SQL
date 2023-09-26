# watcher.py
from time import sleep
from watchdog.observers import Observer
from handler import Handler

class Watcher:
    DIRECTORY_TO_WATCH = r"C:\Users\ChristianStander\Documents\Work\Database practice\Weather\Weather\Test"

    def __init__(self, db_name, table_name, server_name, username, password):
        self.observer = Observer()
        self.db_name = db_name
        self.table_name = table_name
        self.server_name = server_name
        self.username = username
        self.password = password

    def run(self):
        event_handler = Handler(self.db_name, self.table_name, self.server_name, self.username, self.password)
        self.observer.schedule(event_handler, self.DIRECTORY_TO_WATCH, recursive=True)
        self.observer.start()
        try:
            while True:
                sleep(5)
        except:
            self.observer.stop()
            print("Observer Stopped")
        self.observer.join()
