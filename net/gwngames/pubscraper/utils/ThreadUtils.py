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
    def random_sleep(min_seconds: int, max_seconds: int, logger: logging.Logger):
        """Sleep for a random number of seconds between min_seconds and max_seconds."""
        sleep_time = random.uniform(min_seconds, max_seconds)
        logger.info("Waiting {} seconds...".format(sleep_time))
        time.sleep(sleep_time)

    @staticmethod
    def deschedule(config: JsonReader, key: str):

        from net.gwngames.pubscraper.utils.RequestState import RequestState
        min = config.get_value(ConfigConstants.MIN_WAIT_TIME)/5
        max = config.get_value(ConfigConstants.MAX_WAIT_TIME)/5
        logging.info(f"descheduling: {key} for {min}~{max}ms")

        RequestState().notify_update()
        ThreadUtils.random_sleep(min, max)

        RequestState().notify_reschedule(key)
