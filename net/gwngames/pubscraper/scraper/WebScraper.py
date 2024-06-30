import logging
import threading

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scraper.NameFetcher import NameFetcher
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
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
            logging.debug("Massive General Scraping started")

    @staticmethod
    def scrape_interfaces():
        config = JsonReader(JsonReader.CONFIG_FILE_NAME)

        # definition of roots
        scraping_authors: list = []
        author_from: str = config.get_value(ConfigConstants.AUTHORS_REF)
        root_authors: str = config.get_value(ConfigConstants.ROOT_AUTHORS)
        if author_from is not None:
            scraping_authors += NameFetcher.generate_roots(author_from)
        if root_authors is not None:
            scraping_authors += StringUtils.process_string(root_authors)
        #  TODO: add flags in generaldata fetcher to enable roots or smh

        # -------------------

        interfaces: str = config.get_value(ConfigConstants.INTERFACES_ENABLED)
        interface_names = StringUtils.process_string(interfaces)

        for name in interface_names:
            iface: type = GeneralDataFetcher.get_data_fetcher_class(name)
            if iface is None:
                logging.warning(f"Interface {name} is not supported")
                continue
            if isinstance(iface(), ScholarlyDataFetcher):
                    logging.info("Fetching for %s - Authors: %s", iface.__name__, root_authors)
                    ScholarlyDataFetcher(proxy=True).generate_all_relevant_authors(scraping_authors)
                    logging.error("DONE!!!")
                    while True:
                        None
                    #ScholarlyDataFetcher.dispatch_requests(queries)
