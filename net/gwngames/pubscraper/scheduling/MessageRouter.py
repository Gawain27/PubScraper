import datetime
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue
from typing import Any

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.scheduling.MasterPriorityQueue import MasterPriorityQueue
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


duplicate_messages = set()

class PrioritizedTask:
    def __init__(self, priority, task):
        self.priority = priority
        self.task = task

    def __lt__(self, other):
        return self.priority < other.priority

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
        self.task_queue = PriorityQueue()
        threading.Thread(target=self._process_task_queue, daemon=True).start()

    def start(self):
        """
        Starts the processing of messages in a separate thread.
        """
        threading.Thread(target=self.process_messages, daemon=True).start()

    def _process_task_queue(self):
        while True:
            prioritized_task = self.task_queue.get()
            if prioritized_task:
                prioritized_task.task()

    def process_messages(self):
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        while True:
            # Get the next message by priority (lowest first)
            priority, message, message_queue = self.incoming_queue.receive()
            if isinstance(message, AbstractMessage) and isinstance(message_queue, AsyncQueue):
                if message.system_message:
                    # Execute system messages immediately
                    try:
                        self.route_message(message, message_queue)
                    except Exception as e:
                        logging.error(f"Error processing system message: {e}")
                else:
                    # Non-system messages are scheduled
                    self.task_queue.put(PrioritizedTask(1, lambda: self.route_message(message, message_queue)))
            else:
                logging.warning(f"Ignoring message of unknown type: {type(message)}")

    def route_message(self, message: AbstractMessage, message_queue: Any):
        """
        Route a message to a message queue for processing.
        """
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        message_queue: AsyncQueue = message_queue
        if message.system_message:
            self.logger.info("Executing system message %s", message.message_id)
            message_queue.process_message(message)
        else:
            self.logger.info("Scheduling message %s", message.message_id)
            self.executor.submit(message_queue.process_message, message)

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

        if str(message) in duplicate_messages:
            return
        else:
            self.logger.info(f"Scrapping message: {message}")
            duplicate_messages.add(str(message))

        message.priority = priority
        self.incoming_queue.send(priority, message, loaded_queue())

    def send_later_in(self, message: AbstractMessage, priority=0, depth=None):
        message.delayed = True
        message.synchronize = True
        self.send_message(message, priority=priority, depth=depth)

    @staticmethod
    def later_in(data, priority: int, depth: int):
        MessageRouter.get_instance().send_later_in(data, priority, depth)

    @staticmethod
    def get_instance():
        return MessageRouter()

