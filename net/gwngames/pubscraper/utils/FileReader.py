import json
import logging
import threading
from typing import Final


class FileReader:
    """
    A class for reading and getting values from a file.

    :param file: The path to the file.
    """
    CONFIG_FILE_NAME: Final = 'config.json'
    MESSAGE_STAT_FILE_NAME: Final = 'message_stats.json'
    _locks = {}  # Class-level dictionary to hold locks for each file

    def __init__(self, file: str):
        self.file = file
        self.data = None
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)  # Set the logging level to DEBUG

        # Initialize lock for this file if not already present
        if file not in FileReader._locks:
            FileReader._locks[file] = threading.Lock()
        self.lock = FileReader._locks[file]
        self.load_file()

    def load_file(self):
        """
        Load the data from the specified file.

        :return: None
        """
        with self.lock:
            try:
                with open(self.file, 'r') as f:
                    self.data = json.load(f)
                self.logger.info(f"Loaded data from '{self.file}' successfully.")
            except FileNotFoundError:
                self.logger.error(f"Error: File '{self.file}' not found.")
            except json.JSONDecodeError:
                self.logger.error(f"Error: Invalid JSON format in file '{self.file}'.")

    def get_value(self, key: str):
        """
        :param key: The key to look up the value in the configuration data. :return: The value corresponding to the
        given key in the configuration data. If the key is not found, None is returned.

        Raises:
            Exception: If the configuration data has not been loaded. Call `load_file()` first.
        """
        with self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            return self.data.get(key, None)

    def set_value(self, key: str, value):
        """
        Set the value for the specified key in the configuration data.

        :param key: The key to set the value for.
        :param value: The value to set.
        :return: None
        """
        with self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            self.data[key] = value
            self.logger.info(f"Set value '{value}' for key '{key}'.")

    def save_changes(self):
        """
        Save the updated configuration data back to the file.

        :return: None
        """
        with self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            try:
                with open(self.file, 'w') as f:
                    json.dump(self.data, f, indent=4)
                self.logger.info(f"Changes saved to '{self.file}' successfully.")
            except IOError as e:
                self.logger.error(f"Error saving changes to '{self.file}': {e}")

    def set_and_save(self, key: str, value):
        """
        Sets the value of a given key and saves the changes.

        :param key: The key to set the value for.
        :param value: The value to set.
        :return: None
        """
        with self.lock:
            self.set_value(key, value)
            self.save_changes()
