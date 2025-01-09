import atexit
import logging
import os
import sys
import time

import couchdb

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.LogFileHandler import LogFileHandler
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.install_browser import install_browser
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.BanChecker import BanChecker
from com.gwngames.pubscraper.scraper.WebScraper import WebScraper
from com.gwngames.pubscraper.utils.ClassRegisterer import QueueRegisterer
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class ExcludeFilter(logging.Filter):
    def filter(self, record):
        return not any(
            record.name.startswith(mod) for mod in ('httpx', 'httpcore', 'urllib3', 'selenium'))


def on_failure_actions():
    none = 1

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

    # Initialize DB
    client = couchdb.Server( conf_reader.get_value(ConfigConstants.DB_PREFIX) +
        '://' + conf_reader.get_value(ConfigConstants.DB_USER) + ":" + conf_reader.get_value(
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
    log_file_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(log_format)
    console_handler.addFilter(ExcludeFilter())
    console_handler.setLevel(logging.DEBUG)

    logging.basicConfig(level=logging.DEBUG, handlers=[log_file_handler, console_handler])
    logging.propagate = True

    # -- data configuration

    QueueRegisterer().register_queues()

    router = MessageRouter.get_instance()
    router.start()

    if conf_reader.get_value(ConfigConstants.AUTO_ADAPTIVE) is True:
        logging.info("Monitoring scraping state")
        BanChecker(ctx).start_monitoring()

    # Check if embedded browser is required
    if conf_reader.get_value(ConfigConstants.BROWSER_EMBEDDED) is True:
        logging.info("Using embedded browser")
        install_browser()
        current_dir = os.getcwd()
        browser_driver_path = os.path.join(current_dir, "tor_download/tor-browser/Browser/firefox")
        browser_data_path = os.path.join(current_dir,
                                         "tor_download/tor-browser/Browser/TorBrowser/Data/Browser/profile.default")
        gecko_dir = os.path.join(current_dir, "tor_download/geckodriver")
        conf_reader.set_and_save(ConfigConstants.BROWSER_DRIVER_PATH, browser_driver_path)
        conf_reader.set_and_save(ConfigConstants.BROWSER_DATA_PATH, browser_data_path)
        conf_reader.set_and_save(ConfigConstants.BROWSER_TYPE, "embedded")
        conf_reader.set_and_save("geckodriver", gecko_dir)

    scraper = WebScraper()
    scraper.start()  # Asynchronous call, scraper has started

    while True:  # Keep child processes alive
        time.sleep(1000000)
