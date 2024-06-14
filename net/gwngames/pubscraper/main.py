import logging

from net.gwngames.pubscraper.LogFileHandler import LogFileHandler
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scraper.WebScraper import WebScraper
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter

if __name__ == '__main__':
    conf_reader = FileReader(FileReader.CONFIG_FILE_NAME)

    max_logfile_lines: int = conf_reader.get_value(ConfigConstants.MAX_LOGFILE_LINES)

    logging.basicConfig(level=logging.DEBUG,
                        handlers=[LogFileHandler(filename=ConfigConstants.LOG_FILENAME, max_lines=max_logfile_lines)],
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # TODO: all the params flag and stuff
    router = MessageRouter.get_instance()
    router.start()

    scraper = WebScraper()
    scraper.scrape_interfaces()  # Asynchronous call, scraper has started
