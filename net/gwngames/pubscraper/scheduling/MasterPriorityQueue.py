import logging
import queue
import threading
from collections import defaultdict
from typing import Tuple

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.LogFileHandler import LogFileHandler
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage

log_filename = "master_priority_queue.log"
log_handler = LogFileHandler(filename=log_filename, max_lines=10000)

logger = logging.getLogger("MasterPriorityQueue")
logger.setLevel(logging.DEBUG)  # Adjust logging level as needed
logger.addHandler(log_handler)

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
        logger.debug("MasterPriorityQueue initialized with adjustment_interval=%s, avg_depth=%s",
                     self.adjustment_interval, self.avg_depth)

    def send(self, priority: int, message: 'AbstractMessage', subqueue: queue.Queue):
        """
        Enqueue a message with a specific priority and subqueue.
        """
        with self._lock:
            effective_priority = self._calculate_effective_priority(priority, message)
            self.message_type_count[message.message_type] += 1
            self.put((effective_priority, message, subqueue))
            logger.info("Message sent: %s with priority %s (effective priority: %s)",
                        message, priority, effective_priority)

    def receive(self, block: bool = True, timeout: float | None = None) -> Tuple[int, 'AbstractMessage', queue.Queue]:
        """
        Get the highest-priority (lowest-number) item from the queue.
        """
        item = self.get(block, timeout)
        _, message, _ = item
        self.message_type_count[message.message_type] -= 1
        self._message_count += 1
        logger.info("Message received: %s with priority %s", message, item[0])

        # After max_req executed, decrease the priority of all messages in the queue and reset the counter
        if self._message_count >= self.adjustment_interval:
            with self._lock:
                logger.debug("Decreasing priorities for all messages in the queue...")
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
            logger.debug("Adjusted priority for message: %s from %s to %s", message, priority, new_priority)

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
        effective_priority = priority + fairness_penalty + depth_penalty + adjustment
        logger.debug("Calculated effective priority: %s (base: %s, fairness: %s, depth: %s, adjustment: %s)",
                     effective_priority, priority, fairness_penalty, depth_penalty, adjustment)
        return effective_priority

