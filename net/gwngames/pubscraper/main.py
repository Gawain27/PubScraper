import atexit
import logging
import os
import sys

import couchdb
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.LogFileHandler import LogFileHandler
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scheduling.IntegerMap import IntegerMap
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.WebScraper import WebScraper
from net.gwngames.pubscraper.scraper.scraper.ScholarScraper import fetch_publications, get_scholar_profile, \
    fetch_publication_data, scrape_all_citations
from net.gwngames.pubscraper.utils.ClassRegisterer import QueueRegisterer
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class ExcludeFilter(logging.Filter):
    def filter(self, record):
        return not any(record.name.startswith(mod) for mod in ('httpx', 'httpcore', 'urllib3', 'selenium'))


def on_failure_actions():
    IntegerMap().store_all_on_failure()


if __name__ == '__main__':
    # Initialize context
    ctx: Context = Context()
    ctx.set_current_dir(os.getcwd())

    # Initialize files for caching
    conf_reader = JsonReader(JsonReader.CONFIG_FILE_NAME)
    message_stats = JsonReader(JsonReader.MESSAGE_STAT_FILE_NAME)
    ctx.set_config(conf_reader)
    ctx.set_message_data(message_stats)

    # Executes on failure operations
    atexit.register(on_failure_actions)

    #Initialize DB
    client = couchdb.Server('http://' + conf_reader.get_value(ConfigConstants.DB_USER) + ":" + conf_reader.get_value(
        ConfigConstants.DB_PASSWORD)
                            + "@" + str(conf_reader.get_value(ConfigConstants.DB_HOST)) + ':' + str(
        conf_reader.get_value(ConfigConstants.DB_PORT)) + '/')
    ctx.set_client(client)

    max_logfile_lines: int = conf_reader.get_value(ConfigConstants.MAX_LOGFILE_LINES)

    # Create a LogFileHandler for writing logs to a file
    log_file_handler = LogFileHandler(filename=ConfigConstants.LOG_FILENAME, max_lines=max_logfile_lines,
                                      encoding='utf-8')

    console_handler = logging.StreamHandler(sys.stdout)

    log_format = logging.Formatter('[%(asctime)s - %(thread)d]  %(name)s - %(levelname)s: %(message)s')

    log_file_handler.setFormatter(log_format)
    log_file_handler.addFilter(ExcludeFilter())
    console_handler.setFormatter(log_format)
    console_handler.addFilter(ExcludeFilter())

    logging.basicConfig(level=logging.DEBUG, handlers=[log_file_handler, console_handler])

    # -- data configuration

    QueueRegisterer().register_queues()

    router = MessageRouter.get_instance()
    router.start()

    scraper = WebScraper()
    scraper.start()  # Asynchronous call, scraper has started