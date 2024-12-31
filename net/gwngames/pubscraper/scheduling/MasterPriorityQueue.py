import logging
import queue
import threading
from collections import defaultdict
from datetime import datetime, timedelta

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage


class MasterPriorityQueue:
    """
    A singleton priority queue with separate system and process queues.
    System messages are prioritized over process messages, with an option to switch between modes.
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
        self.ctx = Context()
        self.system_queue = queue.PriorityQueue()  # For system messages
        self.process_queue = queue.PriorityQueue()  # For process messages
        self.adjustment_interval = len(self.ctx.get_active_interfaces())
        self.last_adjustment_time = datetime.now()  # Initialize the last adjustment timestamp
        self.max_depth = self.ctx.get_config().get_value(ConfigConstants.DEPTH_MAX)
        self.depth_ordering = self.ctx.get_config().get_value(ConfigConstants.DEPTH_ORDERING)  # Switch mode via ConfigConstants
        self.message_type_count = defaultdict(int)  # Track counts of each message type
        self.logger = logging.getLogger(MasterPriorityQueue.__name__)
        self.logger.debug("MasterPriorityQueue initialized with adjustment_interval=%s, max_depth=%s, depth_ordering=%s",
                     self.adjustment_interval, self.max_depth, str(self.depth_ordering))

    def send(self, priority: int, message: 'AbstractMessage', subqueue: queue.Queue = None):
        """
        Enqueue a message with a specific priority into the appropriate queue.
        """
        if message.depth > self.max_depth != -1:
            self.logger.warning("Depth max reached for: " + message.message_type + "_" + message.message_id)
            return

        effective_priority = self._calculate_effective_priority(priority, message)
        if self.depth_ordering:
            priority_tuple = (message.depth, effective_priority)  # Depth is primary, effective priority is secondary
        else:
            priority_tuple = (effective_priority, message.depth)  # Effective priority is primary

        if message.system_message:
            with self._system_lock:
                self.message_type_count[message.message_type] += 1
                self.system_queue.put((priority_tuple, message, subqueue))
                self.logger.info("System message sent: %s with priority %s (effective priority: %s)",
                                 message, priority, effective_priority)
        else:
            with self._lock:
                self.message_type_count[message.message_type] += 1
                self.process_queue.put((priority_tuple, message, subqueue))
                self.logger.info("Process message sent: %s with priority %s (effective priority: %s) - depth %s",
                                 message, priority, effective_priority, message.depth)

    def receive(self, block: bool = True, timeout: float | None = None) -> tuple:
        """
        Get the highest-priority (lowest-number) item from the system queue first, then the process queue.
        """
        try:
            item = self.system_queue.get(block, 2)  # Try to get from the system queue first
        except queue.Empty:
            item = self.process_queue.get(block, timeout)  # Fall back to the process queue if system queue is empty

        priority_tuple, message, subqueue = item
        self.message_type_count[message.message_type] -= 1
        self._message_count += 1
        self.logger.info("Message received: %s with priority %s and depth %s",
                         message, priority_tuple[1], message.depth)

        # Check if x minute has passed since the last adjustment
        current_time = datetime.now()
        if (current_time - self.last_adjustment_time) >= timedelta(minutes=10):
            with self._lock:
                self.logger.debug("10 minutes have passed. Decreasing priorities for all messages in the queues...")
                self._decrease_priorities()
            self.last_adjustment_time = current_time  # Reset the adjustment timestamp

        return item

    def _decrease_priorities(self):
        """
        Decreases the priority of all messages in both the system and process queues by a depth-based value.
        """
        self._adjust_queue_priorities(self.process_queue)
        self._adjust_queue_priorities(self.system_queue)

    def _adjust_queue_priorities(self, target_queue):
        """
        Decreases the priority of all messages in a specific queue by a depth-based value.
        """
        temp_items = []

        while not target_queue.empty():
            priority_tuple, message, subqueue = target_queue.get()
            new_effective_priority = self._calculate_effective_priority(priority_tuple[1], message, adjust=True)
            if self.depth_ordering:
                new_priority_tuple = (message.depth, new_effective_priority)
            else:
                new_priority_tuple = (new_effective_priority, message.depth)
            temp_items.append((new_priority_tuple, message, subqueue))

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




