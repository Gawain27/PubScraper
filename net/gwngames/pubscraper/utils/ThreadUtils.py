import random
import time


class ThreadUtils:
    @staticmethod
    def random_sleep(min_seconds: int, max_seconds: int):
        """Sleep for a random number of seconds between min_seconds and max_seconds."""
        sleep_time = random.uniform(min_seconds, max_seconds)
        time.sleep(sleep_time)
