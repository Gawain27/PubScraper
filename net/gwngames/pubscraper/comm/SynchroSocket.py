import logging
import socket
import threading

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.utils.FileReader import FileReader


class SynchroSocket:
    _instances = {}
    lock = threading.Lock()
    socket = None
    port = None

    def __new__(cls, port: int):
        with cls.lock:
            if port not in cls._instances:
                cls._instances[port] = super().__new__(cls)
                cls._instances[port].port = port
                cls._instances[port].socket = None  # Initialize socket attribute
                cls._instances[port].lock = threading.Lock()  # Lock for socket operations
                logging.info(f'Created new SynchroSocket instance for port {port}')
            return cls._instances[port]

    def connect_to_java_socket(self):
        with self.lock:
            if not self.socket:
                config = FileReader(FileReader.CONFIG_FILE_NAME)
                try:
                    self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    self.socket.connect((config.get_value(ConfigConstants.SERVER_URL), self.port))
                    logging.info(f'Connected to Java socket on port {self.port}')
                except Exception as e:
                    logging.error(f'Error connecting to Java socket on port {self.port}: {e}')

    def send_message(self, message):
        with self.lock:
            if not self.socket:
                self.connect_to_java_socket()
            try:
                full_message = message + '\n'  # Append newline as message delimiter
                self.socket.sendall(full_message.encode())
                logging.debug(f'Sent message to Java socket on port {self.port}: {message}')
            except Exception as e:
                logging.error(f'Error sending message to Java socket on port {self.port}: {e}')

    def receive_message(self):
        with self.lock:
            if not self.socket:
                self.connect_to_java_socket()

            # Initialize an empty buffer to accumulate received data
            buffer = ''

            while True:
                try:
                    chunk = self.socket.recv(1024).decode()
                    if not chunk:
                        break
                    buffer += chunk

                    # Look for the first complete message in the buffer
                    if '\n' in buffer:
                        # Split the buffer at the first '\n' only
                        message, buffer = buffer.split('\n', 1)

                        # Yield the complete message
                        yield message.strip()

                        # Continue processing the next chunk of data
                        continue

                except Exception as e:
                    logging.error(f'Error receiving message from Java socket on port {self.port}: {e}')
                    break

            # After exiting the loop, yield any remaining message in the buffer
            if buffer:
                yield buffer.strip()
