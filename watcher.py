# watcher.py
from time import sleep
from watchdog.observers import Observer
from handler import InputHandler, OutputHandler

class Watcher:
    INPUT_DIRECTORY = r"path_to_input_directory"
    OUTPUT_DIRECTORY = r"path_to_output_directory"

    def __init__(self, db_name, table_name, server_name, username, password):
        self.input_observer = Observer()
        self.output_observer = Observer()
        self.db_name = db_name
        self.table_name = table_name
        self.server_name = server_name
        self.username = username
        self.password = password

    def run(self):
        input_event_handler = InputHandler(self.OUTPUT_DIRECTORY)
        self.input_observer.schedule(input_event_handler, self.INPUT_DIRECTORY, recursive=True)
        self.input_observer.start()

        output_event_handler = OutputHandler(
            self.db_name, self.table_name, self.server_name, self.username, self.password)
        self.output_observer.schedule(output_event_handler, self.OUTPUT_DIRECTORY, recursive=True)
        self.output_observer.start()

        try:
            while True:
                sleep(5)
        except:
            self.input_observer.stop()
            self.output_observer.stop()
            print("Observers Stopped")

        self.input_observer.join()
        self.output_observer.join()
