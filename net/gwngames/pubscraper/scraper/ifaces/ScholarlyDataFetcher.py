import asyncio
import json
import logging
import os
from datetime import datetime, timedelta
from typing import List, Final, Any

from scholarly import ProxyGenerator
from scholarly import scholarly

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.constants.QueueConstants import QueueConstants
from net.gwngames.pubscraper.msg.scraper.scholarly.GetGoogleScholarData import GetGoogleScholarData
from net.gwngames.pubscraper.msg.scraper.core.ScrapeTopic import ScrapeTopic
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyAuthor import GetScholarlyAuthor
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPublication import GetScholarlyPublication
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.utils.ClassRegisterer import TopicRegisterer
from net.gwngames.pubscraper.utils.FileReader import FileReader
from net.gwngames.pubscraper.utils.FileUtils import FileUtils
from net.gwngames.pubscraper.utils.Semaphore import SingletonSemaphore
from net.gwngames.pubscraper.utils.StringUtils import StringUtils
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class ScholarlyDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = "googlescholar"
    BASE_URL: Final = "https://scholar.google.com"

    def __init__(self, proxy: bool = False):
        super().__init__()  # Assuming GeneralDataFetcher is a parent class
        self.proxy_enabled = proxy

        self.logger = logging.getLogger('ScholarlyDataFetcher')
        logging.basicConfig(level=LoggingConstants.SCHOLARLY_DATA_FETCHER)
        self.config = FileReader(FileReader.CONFIG_FILE_NAME, self.INTERFACE_ID)

        if self.proxy_enabled:
            self.pg = ProxyGenerator()
            self.pg.FreeProxies()
            scholarly.use_proxy(self.pg, self.pg)
            self.logger.info("Proxy enabled using ProxyGenerator")

    def get_new_data_since(self, query: str, date: datetime) -> str:
        self.logger.info("Fetching new data since %s for query: %s", date, query)

        filename = f"{self.INTERFACE_ID}_{query}.json"
        with open(filename, 'w') as file:
            new_data = []
            search_query = scholarly.search_keyword(query)

            for paper in search_query:
                paper_filled = scholarly.fill(paper)
                last_update_str = paper_filled.get('last_update', None)
                if last_update_str:
                    try:
                        last_update = datetime.strptime(last_update_str, '%Y-%m-%d')  # Adjust format if necessary
                        if last_update >= date:
                            new_data.append(paper_filled)
                            json.dump(paper_filled, file)  # Write each paper to file immediately
                            file.write('\n')  # Add newline for better readability
                    except ValueError:
                        self.logger.warning("Date format for last_update is incorrect: %s", last_update_str)
                else:
                    self.logger.warning("No last_update field found for paper: %s",
                                        paper_filled.get('title', 'Unknown'))

        self.logger.info("Saved new data to file: %s", filename)
        return filename

    def generate_all_queries(self, base_query: str, additional_terms: List[str]) -> List[str]:
        self.logger.info("Generating all queries for base query: %s with additional terms: %s", base_query,
                         additional_terms)

        queries = [f"{base_query} {term}" for term in additional_terms]

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{base_query}.json"
        with open(filename, 'w') as file:
            json.dump(queries, file, indent=4)
        self.logger.info("Saved generated queries to file: %s", filename)

        return queries

    def generate_all_relevant_authors(self, authors_list: str):
        self.logger.info("Generating all relevant authors for authors: %s", authors_list)

        authors: list[str] = StringUtils.process_string(authors_list)
        for author in authors:
            author_msg = GetScholarlyAuthor(self.INTERFACE_ID + "_" + author, author)
            MessageRouter.get_instance().send_later_in(author_msg, self.INTERFACE_ID)
            self.logger.info("Sent message %s for author: %s", author_msg.message_id, author)

    def fetch_author_data(self, author: str) -> str:
        self.logger.info("helloooo")
        search_query = scholarly.search_author(author)
        self.logger.info("aiuttooo")
        authors = []

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{author}.json"

        # Supposedly only ever one
        for author_snip in search_query:
            full_author = scholarly.fill(author_snip)
            self.logger.info("Retrieved author: %s", author)

            with open(filename, 'w') as file:
                json.dump(full_author, file, indent=4)
            self.logger.info("Saved generated authors to file: %s", filename)

            for pub in full_author['publications']:
                publication_msg = GetScholarlyPublication(self.INTERFACE_ID + "_" + author, pub)
                MessageRouter.get_instance().send_later_in(publication_msg, self.INTERFACE_ID)
                self.logger.info("Sent publication message %s for author %s", publication_msg.message_id, author)

            authors.append(full_author)

        return filename

    def fetch_author_publication(self, publication: Any) -> str:
        self.logger.info("Fetching author publication for interface %s", self.INTERFACE_ID)
        pub_filled = scholarly.fill(publication)

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{pub_filled['bib']['title']}.json"
        with open(filename, 'w') as file:
            json.dump(pub_filled, file, indent=4)
        self.logger.info("Saved generated authors to file: %s", filename)
        return filename

    def generate_all_relevant_queries(self, base_term: str) -> str:
        current_date = datetime.now().strftime("%Y-%m-%d")
        filehint = f"{ScholarlyDataFetcher.INTERFACE_ID}_{base_term}"

        curr_filename = FileUtils.find(filehint)
        self.logger.debug("Checking whether to save terms to %s...", curr_filename)
        if self.is_generation_outdated(curr_filename, datetime.now()):
            self.logger.debug("Conditions met to save terms. Proceeding...")
            if curr_filename is not None:
                reader = FileReader(curr_filename)
                reader.delete_file()

            curr_filename = filehint + "_" + current_date + ".json"
            reader = FileReader(curr_filename)

            reader.set_and_save(JsonConstants.TAG_TOPIC_BARRIER, True)
            self.logger.debug("Set topic barrier in file: %s", curr_filename)

            self.generate_root_topics(curr_filename)
            self.logger.info("Started generation of relevant terms to file: %s", curr_filename)
        else:
            self.logger.debug("Conditions not met to save terms to %s. Skipping...", filehint)
        return curr_filename

    def generate_root_topics(self, filename: str, number_of_terms: int):
        self.logger.info("Generating all relevant queries for base term: %s with number of terms: %d", filename,
                         number_of_terms)

        search_query = scholarly.search_keyword(filename.split('_')[1])  # interface_keyword_date.json

        try:
            sem = SingletonSemaphore(self.INTERFACE_ID, self.config.get_value(ConfigConstants.MAX_SCHOLARLY_REQUESTS))
            for i in range(number_of_terms):
                sem.acquire()
                self.logger.debug("Fetching next paper in query")
                ThreadUtils.random_sleep(1, 5)
                paper = next(search_query)
                sem.release()
                self.logger.debug("Paper fetched: %s", paper)

                root_topic_msg = ScrapeTopic(filename, number_of_terms, paper)
                MessageRouter.get_instance().send_message(root_topic_msg)
                self.logger.debug("Sent message to scraper queue for paper: %s", paper)

        except StopIteration:
            self.logger.debug("Finished iterating through queries for %s", filename)
        except Exception as e:
            self.logger.error("topics global: " + str(TopicRegisterer().items()))
            self.logger.error("An error occurred during query generation: %s", str(e))
            raise e

    def is_generation_outdated(self, filename, current_date):
        if filename is None:
            return True
        if not os.path.exists(filename):
            self.logger.debug("File %s does not exist. Continuing...", filename)
            return True

        base_filename = os.path.splitext(os.path.basename(filename))[0]
        date_str = base_filename.split('_')[-1]
        try:
            file_date = datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            self.logger.debug("Could not parse date from filename %s. Continuing...", filename)
            return True

        time_difference = current_date - file_date
        days_difference = time_difference / timedelta(days=1)  # Divide timedelta by timedelta(days=1) to get days
        self.logger.debug("File %s exists. Time difference is %.2f days.", filename, days_difference)

        return days_difference > self.config.get_value(ConfigConstants.RESCRAPE_ROOT_TIME)

    @staticmethod
    def dispatch_requests(filename: str):
        # TODO: test execution up until here.
        logging.warning("End of execution for %s!", filename)
        return
        router = MessageRouter.get_instance()
        stats = FileReader(FileReader.MESSAGE_STAT_FILE_NAME)
        query_file = FileReader(filename)
        for query in query_file.get_value(JsonConstants.TAG_TOPIC_SET):
            message = GetGoogleScholarData(ScholarlyDataFetcher.INTERFACE_ID + "_" + query, query)
            first_run = stats.get_value(message.content) is not None
            message.is_first_run = first_run
            stats.set_and_save(message.content, datetime.today())
            router.send_message(message, PriorityConstants.INTERFACE_REQ)

    def scrape_paper(self, msg: ScrapeTopic):
        self.logger.debug("Scraping paper for topic: %s", msg.content)

        try:
            paper_filled = scholarly.fill(msg.paper)
            interests = paper_filled.get('interests', '')  #FIXME change logic
            terms = self.extract_terms(interests)
            self.logger.debug("Total terms read for query %s: %s -> %s", msg.content, len(terms), terms)

            root_topic_file = FileReader(msg.content, self.logger.name)
            self.logger.debug("PAST FILEREADER")
            filtered_terms = terms.difference(TopicRegisterer().items())
            self.logger.debug("terms  read " + str(terms))
            added: bool = TopicRegisterer().register_topic(filtered_terms)
            if not added:
                raise Exception("Exceptioz")
            self.logger.debug("before flush data")
            root_topic_file.flush_data(JsonConstants.TAG_TOPIC_SET, filtered_terms)

            self.logger.debug("Flushed %s -> %s", msg.content, terms)

            if msg.number_of_terms < self.terms_min:
                for term in terms:
                    self.generate_root_topics(self.INTERFACE_ID + "_" + term + "_" + msg.content.split("_")[2],
                                              int(msg.number_of_terms / 2))

        except Exception as e:
            self.logger.error("Error occurred while scraping paper for topic %s: %s", msg.content, str(e))
            self.logger.error("topics global: " + str(TopicRegisterer().items()))
            self.logger.error("filtered_terms: " + str(filtered_terms))
            raise e
