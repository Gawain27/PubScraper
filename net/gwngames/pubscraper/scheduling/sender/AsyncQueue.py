import logging
import queue
import time
from abc import abstractmethod
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.utils.FileReader import FileReader


class AsyncQueue(queue.Queue):

    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize)
        self.message_stats = FileReader(FileReader.MESSAGE_STAT_FILE_NAME)

    def process_message(self, router: MessageRouter, msg: AbstractMessage):
        """
        :param router: The `MessageRouter` object that handles message routing.
        :param msg: The `AbstractMessage` object to process.
        :return: None

        This method is used to process a message by invoking the `on_message` method and removing the message from
        the routing threads list in the provided `router` object.
        """
        start_time: float = time.time()
        logging.debug(f"Message routed for topic '{msg.message_type}': {msg.message_id}")

        self.on_message(msg)  # TODO implement exception catching for logging of errors

        elapsed_time: float = (time.time() - start_time) * 1000
        logging.debug(
            f"Managed message for topic '{msg.message_type}': {msg.message_id} - Time: {elapsed_time:.3f} ms.")
        router.routing_threads.pop(msg.message_id)

    @abstractmethod
    def on_message(self, msg: AbstractMessage) -> None:
        """
        Implementation of the `on_message` method requires static constants and static method calls to business logic
        :param msg: The message received by the method.
        :return: None

        """
        pass
