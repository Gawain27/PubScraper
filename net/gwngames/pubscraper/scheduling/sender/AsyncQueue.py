import logging
import queue
import time
from abc import abstractmethod

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.utils.ClassUtils import ClassUtils
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.RequestState import RequestState


class AsyncQueue(queue.Queue):

    def __init__(self, maxsize: int = 100):
        super().__init__(maxsize)
        self.is_queue_depth_limited = False
        self.ctx = Context()
        self.logger = logging.getLogger(self.register_me().__name__)
        self.logger.setLevel(LoggingConstants.ASYNC_QUEUE)
        self.message_stats = JsonReader(JsonReader.MESSAGE_STAT_FILE_NAME, parent=self.register_me().__name__)
        self.register_queue()

    def process_message(self, router: MessageRouter, msg: AbstractMessage):
        """
        :param router: The `MessageRouter` object that handles message routing.
        :param msg: The `AbstractMessage` object to process.
        :return: None

        This method is used to process a message by invoking the `on_message` method and removing the message from
        the routing threads list in the provided `router` object.
        """
        exception_caught = False

        if self.is_queue_depth_limited:
            if msg.depth is not None and msg.depth > self.ctx.get_config().get_value(ConfigConstants.DEPTH_MAX):
                logging.debug("Max depth reached for message %s - %s", msg.message_type, msg.message_id)
                return
            if msg.depth is None:
                msg.depth = 0

        if msg.delayed:
            router.send_delayed(msg)
            return
        start_time: float = time.time()
        logging.debug(f"Message routed for topic '{msg.message_type}': {msg.message_id}")

        try:
            self.on_message(msg)  # TODO implement exception catching for logging of errors + retry mech
        except Exception as e:
            exception_caught = True
            if isinstance(e, StopIteration):
                logging.error("Reached end of iteration for %s - %s", msg.message_type, msg.message_id)
            else:
                raise e

        if exception_caught:
            msg.prepare_for_retry()

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
