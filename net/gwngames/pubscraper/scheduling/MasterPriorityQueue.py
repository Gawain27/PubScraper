import queue
from typing import Tuple

from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage


class MasterPriorityQueue(queue.PriorityQueue):
    """
    Class MasterPriorityQueue - A singleton priority queue with methods to send and receive prioritized items.
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

    def send(self, priority: int, message: AbstractMessage, subqueue: queue.Queue):
        """
        Enqueue a message with a specific priority and subqueue.
        """
        message.priority = priority
        self.put((priority, message, subqueue))  # Ensures messages are prioritized

    def receive(self, block: bool = True, timeout: float | None = None) -> Tuple[int, 'AbstractMessage', queue.Queue]:
        """
        Get the highest-priority (lowest-number) item from the queue.
        """
        return self.get(block, timeout)
