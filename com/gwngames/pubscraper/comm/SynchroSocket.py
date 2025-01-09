import logging
import socket
import threading
import time

from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class SynchroSocket:
    _instances = {}
    lock = threading.Lock()
    logger = logging.getLogger('SynchroSocket')
    socket = None
    port = None

    def __new__(cls, port: int):
        with cls.lock:
            if port not in cls._instances:
                cls._instances[port] = super().__new__(cls)
                cls._instances[port].port = port
                cls._instances[port].socket = None  # Initialize socket attribute
                cls._instances[port].lock = threading.Lock()  # Lock for socket operations
                SynchroSocket.logger.info(f'Created new SynchroSocket instance for port {port}')
            return cls._instances[port]

    def connect_to_socket(self):
        if not self.socket:
            config = JsonReader(JsonReader.CONFIG_FILE_NAME)
            try:
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.socket.settimeout(30)  # Set timeout for socket operations
                self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 50 * 1024 * 1024)  # Set send buffer size to 50 MB
                self.socket.connect((config.get_value(ConfigConstants.SERVER_URL), self.port))
                SynchroSocket.logger.info(f'Connected to socket on port {self.port}')
            except Exception as e:
                SynchroSocket.logger.error(f'Error connecting to socket on port {self.port}: {e}')

    def reset_socket(self):
        """Reset the socket after a defined number of usages."""
        self.logger.info(f'Resetting socket after successful usage.')
        if self.socket:
            try:
                self.socket.close()
            except Exception as e:
                self.logger.error(f'Error closing socket: {e}')
            finally:
                self.socket = None

    def send_message(self, message: bytes):
        with self.lock:
            if not self.socket:
                self.connect_to_socket()
            try:
                full_message = message + '\n'.encode("utf-8")  # Append newline as message delimiter
                self.socket.sendall(full_message)
                SynchroSocket.logger.info(f'Sent message to socket on port {self.port}: {message}')

                self.reset_socket()

                return
            except ConnectionAbortedError:
                SynchroSocket.logger.info("Connection aborted.")
                time.sleep(3)
            except Exception as e:
                SynchroSocket.logger.error(f'Error sending message to socket on port {self.port}: {e}')
                self.reset_socket()
                raise e
        self.send_message(message)

    def receive_message(self):
        with self.lock:
            if not self.socket:
                self.connect_to_socket()

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
                    SynchroSocket.logger.error(f'Error receiving message from Java socket on port {self.port}: {e}')
                    break

            # After exiting the loop, yield any remaining message in the buffer
            if buffer:
                yield buffer.strip()

