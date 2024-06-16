import logging
import threading
from typing import Dict, Any

from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage
from net.gwngames.pubscraper.scheduling.MasterPriorityQueue import MasterPriorityQueue


class MessageRouter:
    """
    A class that handles routing of messages.

    Methods: - start() Starts the message processing loop in a separate thread. - process_messages() Processes
    incoming messages from the incoming_queue. Routes messages to the appropriate message_queue based on the message
    type. - route_message(message: AbstractMessage, message_queue: AsyncQueue) Routes a message to a specific
    message_queue for processing. - send_message(message: AbstractMessage, message_queue: AsyncQueue, priority=0)
    Sends a message to the incoming_queue for routing. - get_instance() -> MessageRouter Returns the singleton
    instance of the MessageRouter class.

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
        self.incoming_queue = MasterPriorityQueue()
        self.routing_threads: Dict[str, threading.Thread] = {}

    def start(self):
        """
        Starts the processing of messages in a separate thread.

        :return: None
        """
        threading.Thread(target=self.process_messages, daemon=True).start()

    def process_messages(self):
        """
        Process the incoming messages.

        The method receives messages from the incoming queue, routes the messages to the appropriate message queue
        based on their priority, and marks the processed messages as done in the incoming queue.

        :return: This method does not return anything.
        """
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        while True:
            priority, message, message_queue = self.incoming_queue.receive()
            if isinstance(message, AbstractMessage) and isinstance(message_queue, AsyncQueue):
                self.route_message(message, message_queue)
            else:
                logging.warning(f"Ignoring message of unknown type: {type(message)}")
            self.incoming_queue.task_done()

    def route_message(self, message: AbstractMessage, message_type: Any):
        """
        Route a message to a message queue for processing.

        :param message: The message to be routed.
        :param message_type: The message queue to route the message to.
        :return: None
        """
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        message_queue: AsyncQueue = message_type
        self.routing_threads[message.message_id] = threading.Thread(
            target=message_queue.process_message,
            args=(self, message),
            daemon=True
        )
        # TODO don't randomly start threads, maintain a max and release with wait/notify
        self.routing_threads[message.message_id].start()

    def send_message(self, message: AbstractMessage, message_queue: str, priority=0):
        """
        Send a message to a message queue with an optional priority.

        :param message:  An AbstractMessage object representing the message to be sent.
        :param message_queue:  An AsyncQueue object representing the destination message queue.
        :param priority:  An optional integer representing the priority of the message. Defaults to 0.
        :return:  None

        This method sends the given message to the specified message queue with an optional priority.
        It uses the `send` method of the `incoming_queue` to enqueue the message, and logs the successful
        sending of the message using the `logging.info` function.

        Example usage:
            message = Message("Hello, world!")
            queue = AsyncQueue()
            send_message(self, message, queue, priority=1)
        """
        from net.gwngames.pubscraper.scheduling.sender.AsyncQueue import AsyncQueue
        loaded_queue: type = AsyncQueue.get_queue_class(message_queue)
        self.incoming_queue.send(priority, message, loaded_queue())
        logging.info(f"Message sent: {message} to: {message_queue}")

    @staticmethod
    def get_instance():
        return MessageRouter()
