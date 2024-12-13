import logging
import threading

from numpy import random

from net.gwngames.pubscraper.Context import Context
from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scraper.NameFetcher import NameFetcher
from net.gwngames.pubscraper.scraper.ifaces.CoreEduDataFetcher import CoreEduDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.DblpDataFetcher import DblpDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarDataFetcher import ScholarDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScimagoDataFetcher import ScimagoDataFetcher
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.StringUtils import StringUtils


class WebScraper:
    logger = logging.getLogger('WebScraper')

    class SemicolonFoundException(Exception):
        pass

    def __init__(self):
        self.thread = None

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self.scrape_interfaces(), daemon=True)
            self.thread.start()
            WebScraper.logger.debug("Massive General Scraping started")

    @staticmethod
    def scrape_interfaces():
        config = JsonReader(JsonReader.CONFIG_FILE_NAME)

        # definition of roots
        scraping_authors: list = []
        author_from: str = config.get_value(ConfigConstants.AUTHORS_REF)
        root_authors: str = config.get_value(ConfigConstants.ROOT_AUTHORS)
        years = [str(year) for year in range(1999, __import__('datetime').datetime.now().year)]
        if author_from is not None:
            scraping_authors += NameFetcher.generate_roots(author_from)
        if root_authors is not None and root_authors != '':
            scraping_authors += StringUtils.process_string(root_authors)

        if config.get_value(ConfigConstants.SHUFFLE_ROOTS):
            random.shuffle(scraping_authors)
        # -------------------
        interface_names = Context().get_main_interfaces()
        WebScraper.logger.info("Main interfaces enabled: " + str(interface_names))

        for name in interface_names:
            iface: type = GeneralDataFetcher.get_data_fetcher_class(name)
            if iface is None:
                WebScraper.logger.warning(f"Interface {name} is not supported")
                continue

            iface_instance = iface()

            if isinstance(iface_instance, ScholarDataFetcher):
                WebScraper.logger.info("Fetching for %s - Authors: %s", iface.__name__, scraping_authors)
                iface_instance.start_interface_fetching(opt_arg=scraping_authors)
            elif isinstance(iface_instance, DblpDataFetcher):
                WebScraper.logger.info("Fetching for %s - Authors: %s", iface.__name__, scraping_authors)
                iface_instance.start_interface_fetching(opt_arg=scraping_authors)
            elif isinstance(iface_instance, ScimagoDataFetcher):
                WebScraper.logger.info("Fetching for %s - Years: %s", iface.__name__, years)
                iface_instance.start_interface_fetching(opt_arg=years)
            elif isinstance(iface_instance, CoreEduDataFetcher):
                WebScraper.logger.info("Fetching from %s - page: %s", iface.__name__, "1")
                iface_instance.start_interface_fetching(1)  # start from page one
