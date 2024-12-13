import logging
import queue
import threading
from collections import defaultdict
from datetime import datetime, timedelta
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

class MasterPriorityQueue:
    """
    A singleton priority queue with separate system and process queues.
    System messages are prioritized over process messages.
    """
    _instance = None
    _message_count = 0
    _lock = threading.Lock()  # Lock to prevent simultaneous access during priority adjustment
    _system_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(MasterPriorityQueue, cls).__new__(cls, *args, **kwargs)
            cls._instance.__initialized = False
        return cls._instance

    def __init__(self, *args, **kwargs):
        if self.__initialized:
            return
        self.__initialized = True
        self.system_queue = queue.PriorityQueue()  # For system messages
        self.process_queue = queue.PriorityQueue()  # For process messages
        self.ctx = Context()
        self.adjustment_interval = len(self.ctx.get_active_interfaces())
        self.last_adjustment_time = datetime.now()  # Initialize the last adjustment timestamp
        self.max_depth = self.ctx.get_config().get_value(ConfigConstants.DEPTH_MAX)
        self.message_type_count = defaultdict(int)  # Track counts of each message type
        logger.debug("MasterPriorityQueue initialized with adjustment_interval=%s, max_depth=%s",
                     self.adjustment_interval, self.max_depth)

    def send(self, priority: int, message: 'AbstractMessage', subqueue: queue.Queue = None):
        """
        Enqueue a message with a specific priority into the appropriate queue.
        """
        if message.depth > self.max_depth != -1:
            logger.warning("Depth max reached for: " + message.message_type +"_"+message.message_id)
            return

        effective_priority = self._calculate_effective_priority(priority, message)
        if message.system_message:
            with self._system_lock:
                self.message_type_count[message.message_type] += 1
                self.system_queue.put((effective_priority, message, subqueue))
                logger.info("System message sent: %s with priority %s (effective priority: %s)",
                        message, priority, effective_priority)
        else:
            with self._lock:
                self.message_type_count[message.message_type] += 1
                self.process_queue.put((effective_priority, message, subqueue))
                logger.info("Process message sent: %s with priority %s (effective priority: %s) - depth %s",
                        message, priority, effective_priority, message.depth)


    def receive(self, block: bool = True, timeout: float | None = None) -> Tuple[int, 'AbstractMessage', queue.Queue]:
        """
        Get the highest-priority (lowest-number) item from the system queue first, then the process queue.
        """
        try:
            item = self.system_queue.get(block, 2)  # Try to get from the system queue first
        except queue.Empty:
            item = self.process_queue.get(block, timeout)  # Fall back to the process queue if system queue is empty

        _, message, _ = item
        self.message_type_count[message.message_type] -= 1
        self._message_count += 1
        logger.info("Message received: %s with priority %s and depth %s", message, item[0], message.depth)

        # Check if x minute has passed since the last adjustment
        current_time = datetime.now()
        if (current_time - self.last_adjustment_time) >= timedelta(minutes=1):
            with self._lock:
                logger.debug("1 minute has passed. Decreasing priorities for all messages in the queues...")
                self._decrease_priorities()
            self.last_adjustment_time = current_time  # Reset the adjustment timestamp

        return item

    def _decrease_priorities(self):
        """
        Decreases the priority of all messages in both the system and process queues by a depth-based value.
        """
        self._adjust_queue_priorities(self.process_queue, "process")

    def _adjust_queue_priorities(self, target_queue, queue_name):
        """
        Decreases the priority of all messages in a specific queue by a depth-based value.
        """
        temp_items = []

        while not target_queue.empty():
            priority, message, subqueue = target_queue.get()
            new_priority = self._calculate_effective_priority(priority, message, adjust=True)
            temp_items.append((new_priority, message, subqueue))

        for item in temp_items:
            target_queue.put(item)

    def _calculate_effective_priority(self, priority: int, message: 'AbstractMessage', adjust: bool = False) -> int:
        """
        Calculates the effective priority for a message, considering its depth and message type fairness.
        If `adjust` is True, decreases the priority based on the average depth.
        """
        fairness_penalty = self.message_type_count[message.message_type]
        depth_penalty = message.depth if message.depth is not None else 0
        adjustment = -self.max_depth if adjust else 0
        effective_priority = priority + fairness_penalty + depth_penalty + adjustment
        return effective_priority




