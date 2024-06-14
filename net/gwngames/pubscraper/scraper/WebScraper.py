import logging

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

    @staticmethod
    async def scrape_interfaces():
        config = FileReader(FileReader.CONFIG_FILE_NAME)

        interfaces: str = config.get_value(ConfigConstants.INTERFACES_ENABLED)
        terms_max: int = config.get_value(ConfigConstants.TERMS_MAX)
        interface_names = process_string(interfaces)

        for name in interface_names:
            iface: type = GeneralDataFetcher.get_data_fetcher_class(name)
            if iface is None:
                logging.warning(f"Interface {name} is not supported")
            if isinstance(iface, ScholarlyDataFetcher):
                queries = ScholarlyDataFetcher().generate_all_relevant_queries(ScholarlyDataFetcher.BASE_URL, terms_max)
                ScholarlyDataFetcher.dispatch_scholarly_requests(queries)
