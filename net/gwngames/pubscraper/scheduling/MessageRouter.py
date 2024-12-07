import datetime
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.scheduling.MasterPriorityQueue import MasterPriorityQueue
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class MessageRouter:
    """
    A class that handles routing of messages.
    """
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance.__initialized = False
        return cls._instance

    def __init__(self):
        if self.__initialized:
            return
        self.__initialized = True
        self.started_at = datetime.datetime.now()
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME)
        self.incoming_queue = MasterPriorityQueue()
        self.logger = logging.getLogger(MessageRouter.__name__)
        self.MAX_ACTIVE_THREADS = self.config.get_value(ConfigConstants.MAX_ACTIVE_THREADS)
        self.executor = ThreadPoolExecutor(max_workers=self.MAX_ACTIVE_THREADS)

    def start(self):
        """
        Starts the processing of messages in a separate thread.
        """
        threading.Thread(target=self.process_messages, daemon=True).start()

    def process_messages(self):
        """
        Process the incoming messages by priority and submits tasks to a ThreadPoolExecutor.
        """
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        while True:
            # Get the next message by priority (lowest first)
            priority, message, message_queue = self.incoming_queue.receive()
            if isinstance(message, AbstractMessage) and isinstance(message_queue, AsyncQueue):
                # Submit to the thread pool executor for processing
                self.executor.submit(self.route_message, message, message_queue)
            else:
                logging.warning(f"Ignoring message of unknown type: {type(message)}")

    def route_message(self, message: AbstractMessage, message_queue: Any):
        """
        Route a message to a message queue for processing.
        """
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        message_queue: AsyncQueue = message_queue
        message_queue.process_message(message)  # Directly call the queue's process method

    def send_message(self, message: AbstractMessage, priority=0, depth=None):
        """
        Send a message to a message queue with an optional priority.
        """
        if (self.config.get_value(ConfigConstants.MAX_MS_WORKTIME) != -1
                and self.config.get_value(ConfigConstants.MAX_MS_WORKTIME) < (
                        datetime.datetime.now() - self.started_at).total_seconds()
                and message.destination_queue == QueueConstants.SCRAPER_QUEUE):
            self.logger.info(f"Scraping Timeout. Not starting message: {message}")
            return

        if self.config.get_value(ConfigConstants.DEBUG_DELAY):
            time.sleep(10)

        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        loaded_queue: type = AsyncQueue.get_queue_class(message.destination_queue)

        if depth is not None:
            message.depth = depth + 1

        message.priority = priority
        self.incoming_queue.send(priority, message, loaded_queue())

    def send_later_in(self, message: AbstractMessage, priority=0, depth=None):
        message.delayed = True
        message.synchronize = True
        self.send_message(message, priority=priority, depth=depth)

    @staticmethod
    def later_in(data, priority: int):
        MessageRouter.get_instance().send_later_in(data, priority)

    @staticmethod
    def get_instance():
        return MessageRouter()
