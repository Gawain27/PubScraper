import logging
import queue
import time
from abc import abstractmethod

from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.utils.ClassUtils import ClassUtils
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.RequestState import RequestState
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class AsyncQueue(queue.Queue):

    def __init__(self, maxsize: int = 100):
        super().__init__(maxsize)
        self.logger = logging.getLogger(self.register_me().__name__)
        self.logger.setLevel(LoggingConstants.ASYNC_QUEUE)
        self.message_stats = FileReader(FileReader.MESSAGE_STAT_FILE_NAME, self.register_me().__name__)
        self.register_queue()

    def process_message(self, router: MessageRouter, msg: AbstractMessage):
        """
        :param router: The `MessageRouter` object that handles message routing.
        :param msg: The `AbstractMessage` object to process.
        :return: None

        This method is used to process a message by invoking the `on_message` method and removing the message from
        the routing threads list in the provided `router` object.
        """
        if msg.delayed:
            router.send_delayed(msg)
            return
        start_time: float = time.time()
        logging.debug(f"Message routed for topic '{msg.message_type}': {msg.message_id}")

        self.on_message(msg)  # TODO implement exception catching for logging of errors

        elapsed_time: float = (time.time() - start_time) * 1000
        logging.debug(
            f"Managed message for topic '{msg.message_type}': {msg.message_id} - Time: {elapsed_time:.3f} ms.")
        RequestState().notify_update()
        router.routing_threads.pop(msg.message_id)

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
