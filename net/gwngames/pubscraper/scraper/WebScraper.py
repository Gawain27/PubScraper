import logging
import threading

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScholarlyDataFetcher import ScholarlyDataFetcher
from net.gwngames.pubscraper.utils.FileReader import FileReader


def process_string(input_string: str) -> list[str]:
    if ';' in input_string:
        raise WebScraper.SemicolonFoundException("Input string contains a semicolon")
    else:
        # Split the string by commas
        result = input_string.split(',')
        return result


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
        terms_max: int = config.get_value(ConfigConstants.TERMS_MAX)
        interface_names = process_string(interfaces)
        root_topics = config.get_value(ConfigConstants.ROOT_TOPICS)
        root_topics = process_string(root_topics)

        for name in interface_names:
            iface: type = GeneralDataFetcher.get_data_fetcher_class(name)
            if iface is None:
                logging.warning(f"Interface {name} is not supported")
            if isinstance(iface(), ScholarlyDataFetcher):
                for topic in root_topics:
                    logging.info("Generatic topics for %s about %s", iface.__name__, topic)
                    queries = ScholarlyDataFetcher(proxy=True).generate_all_relevant_queries(topic, terms_max)
                    ScholarlyDataFetcher.dispatch_requests(queries)
