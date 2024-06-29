import logging
import threading
from datetime import datetime

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class RequestState:
    _instance = None # todo make request per interface, or per msg type
    _lock = threading.Lock()

    def __init__(self):
        if self.__initialized:
            return
        self.__initialized = True
        self.stored_date = datetime.now()
        self.max_concurrent_requests = FileReader(FileReader.CONFIG_FILE_NAME).get_value(ConfigConstants.MAX_IFACE_REQUESTS)
        self.active_count = 0
        self.logger = logging.getLogger("RequestState")

    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(RequestState, cls).__new__(cls, *args, **kwargs)
                cls._instance.condition = threading.Condition()
                cls._instance.__initialized = False
            return cls._instance

    def update_last_sent(self, min_seconds: int, max_seconds: int, message_type: str):
        with self.condition:
            while (datetime.now() - self.stored_date).total_seconds() < min_seconds:
                if self.active_count < self.max_concurrent_requests:
                    self.active_count += 1
                    break
                else:
                    self.condition.wait()
            ThreadUtils.random_sleep(min_seconds, max_seconds)
            self.logger.info(f"Serving next request {message_type} after waited time {datetime.now() - self.stored_date} - Total: {self.active_count}")
            self.stored_date = datetime.now()

    def notify_update(self):
        with self.condition:
            if self.active_count > 0:
                self.active_count -= 1
                self.condition.notify()

