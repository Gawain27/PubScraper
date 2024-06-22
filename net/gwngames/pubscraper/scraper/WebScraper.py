import logging
import threading

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.StringUtils import StringUtils


class WebScraper:
    class SemicolonFoundException(Exception):
        pass

    def __init__(self):
        self.thread = None

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self.scrape_interfaces(), daemon=True)
            self.thread.start()
            logging.debug("Webscraper instance initialized")

    @staticmethod
    def scrape_interfaces():
        config = FileReader(FileReader.CONFIG_FILE_NAME)

        interfaces: str = config.get_value(ConfigConstants.INTERFACES_ENABLED)
        interface_names = StringUtils.process_string(interfaces)
        root_authors = config.get_value(ConfigConstants.ROOT_AUTHORS)  #FIXME to change with aUTOMATIC

        for name in interface_names:
            iface: type = GeneralDataFetcher.get_data_fetcher_class(name)
            if iface is None:
                logging.warning(f"Interface {name} is not supported")
                continue
            if isinstance(iface(), ScholarlyDataFetcher):
                    logging.info("Fetching for %s - Authors: %s", iface.__name__, root_authors)
                    ScholarlyDataFetcher(proxy=True).generate_all_relevant_authors(root_authors)
                    logging.error("DONE!!!")
                    while True:
                        None
                    #ScholarlyDataFetcher.dispatch_requests(queries)
