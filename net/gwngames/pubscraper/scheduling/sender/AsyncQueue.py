import logging
import queue
import time
import traceback
from abc import abstractmethod

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.utils.ClassUtils import ClassUtils
from net.gwngames.pubscraper.utils.JsonReader import JsonReader

class AsyncQueue(queue.Queue):

    def __init__(self, maxsize: int = 100):
        super().__init__(maxsize)
        self.is_queue_depth_limited = False
        self.ctx = Context()
        self.logger = logging.getLogger(self.register_me().__name__)
        self.logger.setLevel(LoggingConstants.ASYNC_QUEUE)
        self.message_stats = JsonReader(JsonReader.MESSAGE_STAT_FILE_NAME, parent=self.register_me().__name__)
        self.register_queue()

    def process_message(self, msg: AbstractMessage):
        """
        :param msg: The `AbstractMessage` object to process.
        :return: None

        This method is used to process a message by invoking the `on_message` method and removing the message from
        the routing threads list in the provided `router` object.
        """
        retries = self.ctx.get_config().get_value(ConfigConstants.MAX_BUFFER_RETRIES)
        retry_time = self.ctx.get_config().get_value(ConfigConstants.RETRY_TIME_SEC)
        exception_caught = False
        start_time: float = time.time()

        while retries != 0:
            exception_caught = False
            if self.is_queue_depth_limited:
                if msg.depth is None:
                    msg.depth = 0

            self.logger.debug(f"Message routed for topic '{msg.message_type}': {msg.message_id}")

            try:
                self.on_message(msg)
                retries = 0
            except Exception:
                exception_caught = True
                full_exception = traceback.format_exc()
                logging.error(full_exception)

            # FIXME catching and managing connectivity exceptions, but what exceptions??

            if exception_caught is True:
                msg.prepare_for_retry()
                self.logger.error(
                    f"[FAILURE] for topic '{msg.message_type}': {msg.message_id}, retrying in {retry_time} seconds...")
                time.sleep(retry_time)
                retries -= 1

        if exception_caught is True:
            self.logger.error(f"[CRITICAL FAILURE] for topic '{msg.message_type}': {msg.message_id}, aborting...")
            # TODO: add storing mechanism for recovery

        elapsed_time: float = (time.time() - start_time) * 1000
        self.logger.debug(
            f"Managed message for topic '{msg.message_type}': {msg.message_id} - Time: {elapsed_time:.3f} ms.")

    @abstractmethod
    def on_message(self, msg: AbstractMessage) -> None:
        """
        Implementation of the `on_message` method requires static constants and static method calls to business logic
        :param msg: The message received by the method.
        :return: None

        """
        pass

    def register_queue(self) -> None:
        ClassUtils.add_class_to_superclass(self.register_me(), AsyncQueue)

    @abstractmethod
    def register_me(self) -> type:
        pass

    @staticmethod
    def get_queue_class(queue_name: str) -> type:
        for cls in ClassUtils.get_all_subclasses(AsyncQueue):
            if getattr(cls, 'QUEUE') == queue_name:
                return cls
        logging.warning('Queue named %s not found in Queue types list', queue_name)
