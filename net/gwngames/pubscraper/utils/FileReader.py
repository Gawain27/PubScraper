import json
import logging
import os
import threading
from typing import Final, Any, Set


class FileReader:
    """
    A class for reading and getting values from a file.

    :param file: The path to the file.
    """
    CONFIG_FILE_NAME: Final = 'config.json'
    MESSAGE_STAT_FILE_NAME: Final = 'message_stats.json'
    _locks = {}  # Class-level dictionary to hold locks for each file

    def __init__(self, file: str, parent: str = None):
        self.file = file
        self.data = None
        self.logger = logging.getLogger("file_" + file) if parent is None else logging.getLogger(parent + "_" + file)
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
                file_size = os.path.getsize(self.file)

                if file_size == 0:
                    self.data = {}
                    self.logger.info(f"File '{self.file}' is empty. Initialized with empty data.")
                else:
                    with open(self.file, 'r') as f:
                        self.data = json.load(f)
                    self.logger.info(f"Loaded data from '{self.file}' successfully.")
            except FileNotFoundError:
                self.logger.info(f"Creating new file '{self.file}'")
                with open(self.file, 'w') as f:
                    self.data = {}
                    json.dump(self.data, f)
                    self.save_changes()
                    self.logger.info(f"Created new file '{self.file}' and initialized with empty data.")
            except json.JSONDecodeError:
                self.logger.error(f"Error: Invalid JSON format in file '{self.file}'.")

    def get_value(self, key: str):
        """
        Retrieve the value for the specified key from the configuration data.

        :param key: The key to look up.
        :return: The value corresponding to the key. None if key not found.
        """
        if self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")
            self.logger.debug("Retrieving value from for key '%s - %s'.", key, self.data.get(key, None))
            return self.data.get(key, None)

    def set_value(self, key: str, value):
        """
        Set the value for the specified key in the configuration data.

        :param key: The key to set the value for.
        :param value: The value to set.
        :return: None
        """
        if self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            self.data[key] = value
            self.logger.info(f"Set value '{value}' for key '{key}'.")
            self.save_changes()

    def save_changes(self):
        """
        Save the updated configuration data back to the file.

        :return: None
        """
        if self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            try:
                with open(self.file, 'w') as f:
                    json.dump(self.data, f, indent=4)
                self.logger.info(f"Changes saved to '{self.file}' successfully.")
            except IOError as e:
                self.logger.error(f"Error saving changes to '{self.file}': {e}")
        else:
            self.logger.debug(f"No changes saved to '{self.file}' successfully.")

    def clear(self, key: str):
        """
        Clear the specified key from the configuration data.

        :param key: The key to delete.
        :return: None
        """
        with self.lock:
            if self.data is None:
                raise Exception("Data not loaded. Call load_file() first.")

            if key in self.data:
                del self.data[key]
                self.logger.info(f"Cleared key '{key}' from the data.")
                self.save_changes()
            else:
                self.logger.warning(f"Key '{key}' not found in the data.")

    def delete_file(self):
        """
        Delete the entire file associated with this instance of FileReader.

        :return: None
        """
        with self.lock:
            if os.path.exists(self.file):
                with open(self.file, 'r+b') as f:
                    length = f.tell()
                    for _ in range(3):
                        f.seek(0)
                        f.write(os.urandom(length))
                os.remove(self.file)
                self.logger.info(f"{self.file} has been securely deleted.")
            else:
                self.logger.warning(f"{self.file} does not exist.")

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

    def flush_data(self, key: str, value: set[str]) -> None:
        """
        Update data by adding new values and save.

        :param key: The key to update.
        :param value: The values to add.
        :return: None
        """
        self.logger.debug("Flushing data for key: %s", key)

        value_list = list(value)

        with self.lock:
            prev: list[str] = self.get_value(key)
            if prev is None:
                self.set_value(key, value_list)
                self.logger.debug("New data set for key %s: %s", key, value_list)
            else:
                combined_values = list(set(prev) | set(value_list))  # Convert combined set to list
                self.set_value(key, combined_values)
                self.logger.debug("Merged data for key %s: %s -> %s", key, prev, combined_values)
            self.save_changes()

    def increment(self, key: str):
        with self.lock:
            prev = self.get_value(key)
            self.set_value(key, prev + 1)
            self.save_changes()

    def dump_and_save(self, dump: Any = None):
        """
            Save data.

            :param self:
            :param dump: Optional data to append.
            :return: None
            """
        with self.lock:
            with open(self.file, 'a') as f:
                if dump is not None:
                    json.dump(dump, f)
            self.save_changes()
