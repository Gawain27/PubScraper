import datetime
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from queue import PriorityQueue
from typing import Any

from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from com.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from com.gwngames.pubscraper.scheduling.MasterPriorityQueue import MasterPriorityQueue
from com.gwngames.pubscraper.utils.JsonReader import JsonReader
from com.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils

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
        self.logger.info("Initializing MessageRouter...")
        self.MAX_ACTIVE_THREADS = self.config.get_value(ConfigConstants.MAX_ACTIVE_THREADS)
        self.logger.info(f"Configured maximum active threads: {self.MAX_ACTIVE_THREADS}")
        self.executor = ThreadPoolExecutor(max_workers=self.MAX_ACTIVE_THREADS)
        self.task_queue = PriorityQueue()
        threading.Thread(target=self._process_task_queue, daemon=True).start()
        self.logger.info("MessageRouter initialization complete.")

    def start(self):
        """
        Starts the processing of messages in a separate thread.
        """
        self.logger.info("Starting message processing thread...")
        threading.Thread(target=self.process_messages, daemon=True).start()
        self.logger.info("Message processing thread started.")

    def _process_task_queue(self):
        self.logger.info("Starting task queue processor thread...")
        while True:
            prioritized_task = self.task_queue.get()
            if prioritized_task:
                prioritized_task.task()

    def process_messages(self):
        from com.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        self.logger.info("Starting message processing loop...")
        while True:
            priority, message, message_queue = self.incoming_queue.receive()
            if message is None:
                time.sleep(5)
                continue

            if isinstance(message, AbstractMessage) and isinstance(message_queue, AsyncQueue):
                if message.system_message:
                    self.logger.info(f"Processing system message: {message.message_id}")
                    try:
                        self.route_message(message, message_queue)
                    except Exception as e:
                        self.logger.error(f"Error processing system message {message.message_id}: {e}")
                else:
                    self.logger.info(f"Scheduling non-system message: {message.message_id}")
                    self.task_queue.put(PrioritizedTask(1, lambda m=message, mq=message_queue: self.route_message(m, mq)))
            else:
                self.logger.warning(f"Ignoring message of unknown type: {type(message)}")

    def route_message(self, message: AbstractMessage, message_queue: Any):
        """
        Route a message to a message queue for processing.
        """
        from com.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        self.logger.debug(f"Routing message {message.message_id} to the appropriate queue.")
        message_queue: AsyncQueue = message_queue
        if message.system_message:
            self.logger.info(f"Executing system message: {message.message_id}")
            message_queue.process_message(message)
        else:
            self.executor.submit(message_queue.process_message, message)

    def send_message(self, message: AbstractMessage, priority: int, delay_min: int = 0, delay_max: int = 0):
        """
        Send a message to a message queue with an optional priority.
        """
        if (self.config.get_value(ConfigConstants.MAX_MS_WORKTIME) != -1
                and self.config.get_value(ConfigConstants.MAX_MS_WORKTIME) < (
                        datetime.datetime.now() - self.started_at).total_seconds()
                and message.destination_queue == QueueConstants.SCRAPER_QUEUE):
            self.logger.warning(f"Scraping timeout reached. Not sending message: {message}")
            return

        if self.config.get_value(ConfigConstants.DEBUG_DELAY):
            self.logger.debug("Debug delay enabled. Sleeping for 10 seconds before sending message.")
            time.sleep(10)

        if message.delayed:
            ThreadUtils.sleep_for(delay_min, delay_max, self.logger, message.message_id)

        from com.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        loaded_queue: type = AsyncQueue.get_queue_class(message.destination_queue)

        if message.depth is not None:
            message.depth = message.depth + 1
            self.logger.debug(f"Incremented message {message} depth to: {message.depth}")

        if message.system_message is not True and str(message) in duplicate_messages:
            self.logger.info(f"Duplicate message detected. Scrapping message: {message}")
            return
        else:
            duplicate_messages.add(str(message))
            self.logger.debug(f"Message added to duplicate tracker: {message}")

        message.priority = priority
        self.logger.info(f"Sending message {message.message_id} to incoming queue with priority {priority}.")
        self.incoming_queue.send(priority, message, loaded_queue())

    def send_later_in(self, message: AbstractMessage, priority: int, delay_min: int = 0, delay_max: int = 0):
        message.delayed = True
        def send_message_thread():
            self.send_message(message, priority=priority, delay_min=delay_min, delay_max=delay_max)

        thread = threading.Thread(target=send_message_thread)
        thread.start()

    @staticmethod
    def later_in(data, priority: int, delay_min: int = 0, delay_max: int = 0):
        MessageRouter.get_instance().send_later_in(data, priority, delay_min, delay_max)

    @staticmethod
    def get_instance():
        return MessageRouter()

