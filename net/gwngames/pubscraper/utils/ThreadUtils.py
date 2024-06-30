import logging
import random
import threading
import time
from datetime import datetime
from typing import Callable, Any

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class ThreadUtils:

    @staticmethod
    def random_sleep(min_seconds: int, max_seconds: int):
        """Sleep for a random number of seconds between min_seconds and max_seconds."""
        sleep_time = random.uniform(min_seconds, max_seconds)
        time.sleep(sleep_time)
