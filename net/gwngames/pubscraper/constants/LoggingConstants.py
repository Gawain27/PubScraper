import logging
from typing import Final


class LoggingConstants:
    # Queues must have same logging level
    ASYNC_QUEUE: Final = logging.DEBUG

    GENERAL_DATA_FETCHER: Final = logging.DEBUG
    SCHOLARLY_DATA_FETCHER: Final = logging.DEBUG

    WEBSCRAPER: Final = logging.DEBUG