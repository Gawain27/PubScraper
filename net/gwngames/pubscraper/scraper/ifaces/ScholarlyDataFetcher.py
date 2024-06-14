import json
import logging
from datetime import datetime
from typing import List, Dict, Set, Final

from scholarly import scholarly
from scholarly import ProxyGenerator

from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.GetGoogleScholarData import GetGoogleScholarData
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scheduling.sender.ScraperQueue import ScraperQueue
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.utils.FileReader import FileReader


class ScholarlyDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = "googlescholar"
    BASE_URL: Final = "https://scholar.google.com"

    def __init__(self, proxy: bool = False):
        super().__init__()  # Assuming GeneralDataFetcher is a parent class
        self.proxy_enabled = proxy
        if self.proxy_enabled:
            self.pg = ProxyGenerator()
            self.pg.FreeProxies()
            scholarly.use_proxy(self.pg)
            logging.info("ScholarlyDataFetcher proxy enabled using ProxyGenerator")

    def get_new_data_since(self, query: str, date: datetime) -> str:
        logging.info("Fetching new data since %s for query: %s", date, query)

        search_query = scholarly.search_pubs(query)
        new_data = []

        for paper in search_query:
            paper_filled = scholarly.fill(paper)
            pub_date = datetime.strptime(str(paper_filled['pub_year']), '%Y')
            if pub_date >= date:
                new_data.append(paper_filled)

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{query}.json"
        with open(filename, 'w') as file:
            json.dump(new_data, file, indent=4)
        logging.info("Saved new data to file: %s", filename)

        return filename

    def generate_all_queries(self, base_query: str, additional_terms: List[str]) -> List[str]:
        logging.info("Generating all queries for base query: %s with additional terms: %s", base_query,
                     additional_terms)

        queries = [f"{base_query} {term}" for term in additional_terms]

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{base_query}.json"
        with open(filename, 'w') as file:
            json.dump(queries, file, indent=4)
        logging.info("Saved generated queries to file: %s", filename)

        return queries

    def generate_all_relevant_queries(self, base_query: str, number_of_terms: int = 10) -> List[str]:
        logging.info("Generating all relevant queries for base query: %s with number of terms: %d", base_query,
                     number_of_terms)

        search_query = scholarly.search_pubs(base_query)
        terms: Set[str] = set()

        for _ in range(number_of_terms):
            paper = next(search_query)
            paper_filled = scholarly.fill(paper)
            abstract = paper_filled.get('abstract', '')
            terms.update(self.extract_terms(abstract))
            logging.debug("Terms read for query %s: %s", search_query, len(terms))
            if len(terms) >= number_of_terms:
                break

        relevant_queries = [f"{base_query} {term}" for term in list(terms)[:number_of_terms]]

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{base_query}.json"
        with open(filename, 'w') as file:
            json.dump(relevant_queries, file, indent=4)
        logging.info("Saved relevant queries to file: %s", filename)

        return relevant_queries

    @staticmethod
    def dispatch_scholarly_requests(queries: List[str]):
        router: MessageRouter = MessageRouter()
        stats: FileReader = FileReader(FileReader.MESSAGE_STAT_FILE_NAME)
        for query in queries:
            message = GetGoogleScholarData(ScholarlyDataFetcher.INTERFACE_ID+"_"+query, query)
            first_run: bool = stats.get_value(message.content) is not None
            message.is_first_run = first_run
            router.send_message(message, ScraperQueue(), PriorityConstants.INTERFACE_REQ)
