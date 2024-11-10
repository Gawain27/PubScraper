import queue
import threading
from typing import Tuple

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage


class MasterPriorityQueue(queue.PriorityQueue):
    """
    Class MasterPriorityQueue - A singleton priority queue with methods to send and receive prioritized items.
    """
    _instance = None
    _message_count = 0
    _lock = threading.Lock()  # Lock to prevent simultaneous access during priority adjustment

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
        self.ctx = Context()
        self.adjustment_interval = len(self.ctx.get_active_interfaces())
        self.avg_depth = self.ctx.get_config().get_value(ConfigConstants.DEPTH_MAX)

    def send(self, priority: int, message: 'AbstractMessage', subqueue: queue.Queue):
        """
        Enqueue a message with a specific priority and subqueue.
        """
        with self._lock:
            message.priority = priority
            self.put((priority, message, subqueue))

    def receive(self, block: bool = True, timeout: float | None = None) -> Tuple[int, 'AbstractMessage', queue.Queue]:
        """
        Get the highest-priority (lowest-number) item from the queue.
        """
        item = self.get(block, timeout)
        self._message_count += 1

        # After max_req executed, decrease the priority of all messages in the queue and reset the counter
        if self._message_count >= self.adjustment_interval:
            with self._lock:
                self._decrease_priorities()
            self._message_count = 0

        return item

    def _decrease_priorities(self):
        """
        Decreases the priority of all messages in the queue by a depth based value.
        """
        temp_items = []

        while not self.empty():
            priority, message, subqueue = self.get()
            message: AbstractMessage
            new_priority = priority - self.avg_depth + message.depth
            temp_items.append((new_priority, message, subqueue))

        for item in temp_items:
            self.put(item)
