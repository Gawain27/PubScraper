from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage


import queue
import threading
from typing import Tuple
from collections import defaultdict


class MasterPriorityQueue(queue.PriorityQueue):
    """
    A singleton priority queue with methods to send and receive prioritized items,
    ensuring fairness for message types and preference for smaller depths.
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
        self.message_type_count = defaultdict(int)  # Track counts of each message type

    def send(self, priority: int, message: 'AbstractMessage', subqueue: queue.Queue):
        """
        Enqueue a message with a specific priority and subqueue.
        """
        with self._lock:
            self.message_type_count[message.message_type] += 1
            effective_priority = self._calculate_effective_priority(priority, message)
            self.put((effective_priority, message, subqueue))

    def receive(self, block: bool = True, timeout: float | None = None) -> Tuple[int, 'AbstractMessage', queue.Queue]:
        """
        Get the highest-priority (lowest-number) item from the queue.
        """
        item = self.get(block, timeout)
        _, message, _ = item
        self.message_type_count[message.message_type] -= 1
        self._message_count += 1

        # After max_req executed, decrease the priority of all messages in the queue and reset the counter
        if self._message_count >= self.adjustment_interval:
            with self._lock:
                self._decrease_priorities()
            self._message_count = 0

        return item

    def _decrease_priorities(self):
        """
        Decreases the priority of all messages in the queue by a depth-based value,
        ensuring fairness across message types.
        """
        temp_items = []

        while not self.empty():
            priority, message, subqueue = self.get()
            new_priority = self._calculate_effective_priority(priority, message, adjust=True)
            temp_items.append((new_priority, message, subqueue))

        for item in temp_items:
            self.put(item)

    def _calculate_effective_priority(self, priority: int, message: 'AbstractMessage', adjust: bool = False) -> int:
        """
        Calculates the effective priority for a message, considering its depth and message type fairness.
        If `adjust` is True, decreases the priority based on the average depth.
        """
        fairness_penalty = self.message_type_count[message.message_type]
        depth_penalty = message.depth if message.depth is not None else 0
        adjustment = -self.avg_depth if adjust else 0
        # Lower effective priority means higher priority in the queue
        return priority + fairness_penalty + depth_penalty + adjustment
