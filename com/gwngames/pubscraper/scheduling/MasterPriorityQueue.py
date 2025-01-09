import heapq
import logging
import queue
import threading
from collections import defaultdict

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.msg.AbstractMessage import AbstractMessage

import heapq
import threading
import logging
from collections import defaultdict
from typing import Optional


class MasterPriorityQueue:
    """
    A singleton priority queue with separate system and process queues.
    System messages are prioritized over process messages, with depth as the primary ordering factor.
    """
    _instance = None
    _lock = threading.Lock()
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
        self.max_depth = self.ctx.get_config().get_value(ConfigConstants.DEPTH_MAX)
        self.system_queue = []  # Using a list with heapq for system messages
        self.process_queue = []  # Using a list with heapq for process messages
        self.message_type_count = defaultdict(int)
        self.logger = logging.getLogger(MasterPriorityQueue.__name__)
        self.processed_message_count = 0  # Count of processed messages
        self.logger.debug("MasterPriorityQueue initialized.")

    def _decrease_priorities(self, queue_to_decrease):
        """
        Decrease the priorities of all messages in the given queue by 1.
        """
        self.logger.debug("Decreasing priorities for queue: %s", queue_to_decrease)
        for i in range(len(queue_to_decrease)):
            priority_tuple, message, subqueue = queue_to_decrease[i]
            new_priority_tuple = (priority_tuple[0], priority_tuple[1] - 1, priority_tuple[2])
            queue_to_decrease[i] = (new_priority_tuple, message, subqueue)
        heapq.heapify(queue_to_decrease)  # Reorder the heap
        self.logger.debug("Priorities decreased and heap restructured.")

    def _check_and_adjust_priorities(self):
        """
        Check the processed message count and decrease priorities if threshold is met.
        """
        if self.processed_message_count % 100 == 0:
            self.logger.info("Processed message count reached threshold: %s", self.processed_message_count)
            with self._system_lock:
                self.logger.debug("Lock acquired for system queue priority adjustment.")
                self._decrease_priorities(self.system_queue)
                self.logger.debug("System queue priorities adjusted.")

            with self._lock:
                self.logger.debug("Lock acquired for process queue priority adjustment.")
                self._decrease_priorities(self.process_queue)
                self.logger.debug("Process queue priorities adjusted.")

            self.logger.info("Decreased priorities of all messages after processing 100 messages.")

    def send(self, priority: int, message: 'AbstractMessage', subqueue: Optional[queue.Queue] = None):
        if message.depth > self.max_depth:
            self.logger.warning("Depth max reached for: %s_%s", message.message_type, message.message_id)
            return

        priority_tuple = (message.depth, priority, -message.timestamp.timestamp())

        self.logger.debug("Preparing to send message: %s with priority tuple: %s", message, priority_tuple)

        if message.system_message:
            with self._system_lock:
                self.message_type_count[message.message_type] += 1
                heapq.heappush(self.system_queue, (priority_tuple, message, subqueue))
                self.logger.info("System message sent: %s with priority %s (depth: %s, timestamp: %s)",
                                 message, priority, message.depth, message.timestamp)
        else:
            with self._lock:
                self.message_type_count[message.message_type] += 1
                heapq.heappush(self.process_queue, (priority_tuple, message, subqueue))
                self.logger.info("Process message sent: %s with priority %s (depth: %s, timestamp: %s)",
                                 message, priority, message.depth, message.timestamp)

    def receive(self) -> tuple:
        with self._system_lock:
            if self.system_queue:
                item = heapq.heappop(self.system_queue)
            else:
                item = None

        if not item:
            with self._lock:
                if self.process_queue:
                    item = heapq.heappop(self.process_queue)
                else:
                    return None, None, None

        priority_tuple, message, subqueue = item
        self.message_type_count[message.message_type] -= 1
        self.processed_message_count += 1
        self.logger.info("Message received: %s with priority %s, depth %s, and timestamp %s",
                         message, priority_tuple[1], message.depth, message.timestamp)

        self._check_and_adjust_priorities()

        return priority_tuple[1], message, subqueue





