import logging
import queue
from typing import Tuple

from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage


class MasterPriorityQueue(queue.PriorityQueue):
    """
    Class MasterPriorityQueue

    This class extends the `queue.PriorityQueue` class and provides additional methods for putting and getting elements.
    It is implemented as a singleton.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MasterPriorityQueue, cls).__new__(cls, *args, **kwargs)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, *args, **kwargs):
        if self.__initialized:
            return
        super(MasterPriorityQueue, self).__init__(*args, **kwargs)
        self.__initialized = True

    def send(self, priority: int, message: 'AbstractMessage', subqueue: queue.Queue):
        """
        Put method for MasterPriorityQueue.

        :param subqueue: the queue that will process the message correctly
        :param priority: the priority of the tuple to be put into the priority queue
        :param message: the AbstractMessage object to be put into the priority queue
        :return: None
        """
        logging.debug(
            f"Putting (priority={priority}, message={message}, subqueue={subqueue}) into the queue")
        queue.PriorityQueue.put(self, (priority, message, subqueue))

    def receive(self, block: bool = True, timeout: float | None = None) -> Tuple[int, 'AbstractMessage', queue.Queue]:
        """
        Get the highest priority item from the queue.

        :param block: If set to True (default), the method will block until an item is available.
                      If set to False, it will raise an Empty exception if the queue is empty.
        :param timeout: If set to a float value greater than 0, the method will block for at most the given
                        number of seconds until an item is available.
                        If set to None (default), it will block indefinitely until an item is available.
        :return: A tuple (priority, message, priority_queue) where priority is the priority of the item,
                 message is the AbstractMessage object associated with the item,
                 and priority_queue is the PriorityQueue object associated with the item.
        """
        priority, message, subqueue = queue.PriorityQueue.get(self, block, timeout)
        logging.debug(
            f"Getting (priority={priority}, message={message}, subqueue={subqueue}) from the queue")
        return priority, message, subqueue