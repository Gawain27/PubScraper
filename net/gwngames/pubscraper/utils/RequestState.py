import logging
import threading
from datetime import datetime

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class RequestState:
    _instance = None # todo make request per interface, or per msg type
    _lock = threading.Lock()

    def __init__(self):
        with self._lock:
            if self.__initialized:
                return
            self.__initialized = True
            self.stored_date = datetime.now()
            self.max_concurrent_requests = JsonReader(JsonReader.CONFIG_FILE_NAME).get_value(
                ConfigConstants.MAX_IFACE_REQUESTS)
            self.active_count = 0
            self.logger = logging.getLogger("RequestState")
            self.condition = threading.Condition()

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:  # Double-checked locking
                    cls._instance = super(RequestState, cls).__new__(cls, *args, **kwargs)
                    cls._instance.__initialized = False  # Ensure initialization flag is set before init
        return cls._instance

    def update_last_sent(self, min_seconds: int, max_seconds: int, message_id: str, message_type):
        with self.condition:
            while (datetime.now() - self.stored_date).total_seconds() < min_seconds:
                if self.active_count < self.max_concurrent_requests:
                    self.active_count += 1
                    self.logger.info("breaking")
                    break
                else:
                    self.logger.info("waiting")
                    self.condition.wait()
        ThreadUtils.random_sleep(min_seconds, max_seconds, self.logger)
        self.logger.info(f"Serving next request {message_type} - {message_id} after waited time {datetime.now() - self.stored_date} - Total: {self.active_count}")
        self.stored_date = datetime.now()

    def notify_reschedule(self, key):
        with self.condition:
            while True:
                if self.active_count < self.max_concurrent_requests:
                    self.logger.info(f"Rescheduled message {key}")
                    self.active_count += 1
                    break
                else:
                    self.condition.wait()

    def notify_update(self):
        with self.condition:
            if self.active_count > 0:
                self.active_count -= 1
                self.condition.notify_all()

