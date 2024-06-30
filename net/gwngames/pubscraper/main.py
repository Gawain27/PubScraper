import logging
import os
import sys
import time

import scholarly
from fp.fp import FreeProxy
from scholarly import ProxyGenerator

from net.gwngames.pubscraper.LogFileHandler import LogFileHandler
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scraper.NameFetcher import NameFetcher
from net.gwngames.pubscraper.scraper.WebScraper import WebScraper
from net.gwngames.pubscraper.utils.ClassRegisterer import QueueRegisterer
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter


class ExcludeFilter(logging.Filter):
    def filter(self, record):
        return not any(record.name.startswith(mod) for mod in ('httpx', 'httpcore', 'urllib3', 'selenium', 'scholarly'))


if __name__ == '__main__':
    conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)

    max_logfile_lines: int = conf_reader.get_value(ConfigConstants.MAX_LOGFILE_LINES)

    # Create a LogFileHandler for writing logs to a file
    log_file_handler = LogFileHandler(filename=ConfigConstants.LOG_FILENAME, max_lines=max_logfile_lines)

    console_handler = logging.StreamHandler(sys.stdout)

    log_format = logging.Formatter('[%(asctime)s - %(thread)d]  %(name)s - %(levelname)s: %(message)s')

    log_file_handler.setFormatter(log_format)
    log_file_handler.addFilter(ExcludeFilter())
    console_handler.setFormatter(log_format)
    console_handler.addFilter(ExcludeFilter())

    logging.basicConfig(level=logging.DEBUG, handlers=[log_file_handler, console_handler])

    # -- data configuration

    QueueRegisterer().register_queues()

    # TODO: all the params flag and stuff
    router = MessageRouter.get_instance()
    router.start()

    scraper = WebScraper()
    scraper.start()  # Asynchronous call, scraper has started
