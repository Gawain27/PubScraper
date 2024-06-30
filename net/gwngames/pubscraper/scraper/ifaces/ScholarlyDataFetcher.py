import logging
from typing import Final, Any

from scholarly import ProxyGenerator
from scholarly import scholarly

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyAuthor import GetScholarlyAuthor
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubCitations import GetScholarlyPubCitations
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubRelatedArticles import GetScholarlyPubRelatedArticles
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPublication import GetScholarlyPublication
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.utils.FileReader import FileReader


class ScholarlyDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = "googlescholar"
    BASE_URL: Final = "https://scholar.google.com"
    PUBLICATION_SALT: Final = ['author_pub_id', 'bib.pub_year', 'bib.title']
    PROXY_STARTED: bool = False

    def __init__(self, proxy: bool = False):
        super().__init__()  # Assuming GeneralDataFetcher is a parent class
        self.proxy_enabled = proxy

        self.logger = logging.getLogger('ScholarlyDataFetcher')
        logging.basicConfig(level=LoggingConstants.SCHOLARLY_DATA_FETCHER)
        self.config = FileReader(FileReader.CONFIG_FILE_NAME, self.INTERFACE_ID)

        if self.proxy_enabled and not ScholarlyDataFetcher.PROXY_STARTED:
            self._set_scraperapi_proxy()
            ScholarlyDataFetcher.PROXY_STARTED = True

    def _set_scraperapi_proxy(self):
        pg = ProxyGenerator()
        key: str = self.config.get_value(ConfigConstants.SCRAPERAPI_KEY)
        if key is None:
            raise Exception("A Scraper API key must be provided for scholarly to work!")
        success = pg.ScraperAPI(key)
        if not success:
            raise Exception("Proxy could not be fetched")
        scholarly.use_proxy(pg)

    def generate_all_relevant_authors(self, authors_list: list):
        self.logger.info("Generating all relevant authors for authors: %s", authors_list)

        authors: list[str] = authors_list
        for author in authors:
            author_msg = GetScholarlyAuthor(self.INTERFACE_ID + "_" + author, author)
            MessageRouter.get_instance().send_later_in(author_msg, self.INTERFACE_ID)
            self.logger.info("Sent message %s for author: %s", author_msg.message_id, author)

    def fetch_author_data(self, author: str) -> str:
        search_query = scholarly.search_author(author)

        filename = f"{ScholarlyDataFetcher.INTERFACE_ID}_{author}.json"
        # Supposedly only ever one
        for author_snip in search_query:
            existing_author = FileReader(filename)
            if existing_author.is_empty() or existing_author.is_outdated():
                existing_author.load_file(create=True)
                full_author = scholarly.fill(author_snip)
                self.logger.info("Retrieved author: %s", author)

                existing_author.dump_and_save(full_author)
            else:
                full_author = existing_author.data
                filename = "/dev/null"
                self.logger.info("Loaded up to date author %s", author)

            # This must be synchronous, recheck each publication to check for re-scrape
            pub_number: int = 0
            for pub in full_author['publications']:
                pub_filename = f"{self.generate_unique_key(self.INTERFACE_ID, pub, self.PUBLICATION_SALT)}_pub.json"
                pub_file = FileReader(pub_filename)
                if pub_number >= self.config.get_value(ConfigConstants.MAX_FETCHABLE):
                    # todo bring this log to general fetcher, add joining of ops before notifying completion
                    self.logger.info("Scraped all publications for this run for Author: %s", author)
                    break
                if not pub_file.is_empty() and not pub_file.is_outdated():
                    self.logger.info("file %s is up to date", pub_filename)
                    publication_id: str = pub['author_pub_id']
                    citations_msg = GetScholarlyPubCitations(self.INTERFACE_ID + "_" + publication_id + "_citation",
                                                             publication_id, None, pub_file.data)
                    MessageRouter.get_instance().send_later_in(citations_msg, self.INTERFACE_ID)

                    articles_msg = GetScholarlyPubRelatedArticles(self.INTERFACE_ID + "_" + publication_id + "_article",
                                                                  publication_id, None, pub_file.data)
                    MessageRouter.get_instance().send_later_in(articles_msg, self.INTERFACE_ID)
                    continue
                pub_number += 1

                publication_msg = GetScholarlyPublication(pub_filename, pub)
                MessageRouter.get_instance().send_later_in(publication_msg, self.INTERFACE_ID)
                self.logger.info("Sent publication message for pub %s for author %s", filename, author)

        return filename

    pub_cit_num: dict[str, int] = {}
    cit_starting_index: dict[str, int] = []

    def fetch_pub_citations(self, iter_key: str, citations: Any, pub_id: str, pub: Any = None) -> str:
        filename = f"{self.INTERFACE_ID}_{pub_id}_citation.json"
        citations_file: FileReader = FileReader(filename)
        if citations is None:
            assert pub is not None
            citations = scholarly.citedby(pub)
            MessageRouter.get_instance().send_later_in(GetScholarlyPubCitations(filename, pub_id, citations),
                                                       self.INTERFACE_ID)
            self.pub_cit_num[filename] = 0
            self.logger.info(iter_key)
            self.cit_starting_index[filename] = int(FileReader(FileReader.MESSAGE_STAT_FILE_NAME).get_value(iter_key)[0])-1
            self.logger.info("Scraping citations for %s starting at index %d", iter_key, self.cit_starting_index[filename])
            return "/dev/null"

        self.logger.info("Fetching citation %s - %s", self.pub_cit_num[filename], pub_id)
        # TODO: add outdated check for citations

        if self.pub_cit_num[filename] > self.config.get_value(ConfigConstants.MAX_FETCHABLE):
            self.logger.warning("Reached max number of citations fetchable for %s", filename)
            return "/dev/null"
        citation = next(citations, self.cit_starting_index[filename])
        if self.cit_starting_index[filename] is not None:
            self.cit_starting_index[filename] = None

        unique_citation: str = self.generate_unique_key("", citation, self.PUBLICATION_SALT)
        if citations_file.get_value(unique_citation) is None:
            citations_file.set_and_save(unique_citation, citation)
            self.logger.info("Saved citation %s to file: %s", self.pub_cit_num[filename], filename)
            self.pub_cit_num[filename] += 1
        else:
            self.logger.info("citation: %s is up to date", unique_citation)

        MessageRouter.get_instance().send_later_in(GetScholarlyPubCitations(filename, pub_id, citations),
                                                   self.INTERFACE_ID)
        return filename

    pub_art_num: dict[str, int] = {}
    art_starting_index: dict[str, int] = []

    def fetch_related_articles(self, iter_key: str, articles: Any, pub_id: str, pub: Any = None) -> str:
        filename = f"{self.INTERFACE_ID}_{pub_id}_related.json"
        articles_file: FileReader = FileReader(filename)
        if articles is None:
            assert pub is not None
            articles = scholarly.get_related_articles(pub)
            MessageRouter.get_instance().send_later_in(GetScholarlyPubRelatedArticles(filename, pub_id, articles),
                                                       self.INTERFACE_ID)
            self.pub_art_num[filename] = 0
            self.logger.info(iter_key)
            self.art_starting_index[filename] = int(FileReader(FileReader.MESSAGE_STAT_FILE_NAME).get_value(iter_key)[0])-1
            self.logger.info("Scraping articles for %s starting at index %d", iter_key, self.art_starting_index[filename])
            return "/dev/null"

        self.logger.info("Fetching article %s - %s", self.pub_art_num[filename], pub_id)
        # TODO: add outdated check for articles
        if self.pub_art_num[filename] > self.config.get_value(ConfigConstants.MAX_FETCHABLE):
            self.logger.warning("Reached max number of articles fetchable for %s", filename)
            return "/dev/null"

        article = next(articles, self.art_starting_index[filename])
        if self.art_starting_index[filename] is not None:
            self.art_starting_index[filename] = None

        unique_article: str = self.generate_unique_key("", article, self.PUBLICATION_SALT)
        if articles_file.get_value(unique_article) is None:
            articles_file.set_and_save(unique_article, article)
            self.logger.info("Saved article %s to file: %s", unique_article, filename)
            self.pub_art_num[filename] += 1
        else:
            self.logger.info("article: %s is up to date", unique_article)

        MessageRouter.get_instance().send_later_in(GetScholarlyPubRelatedArticles(filename, pub_id, articles),
                                                   self.INTERFACE_ID)
        return filename

    def fetch_author_publication(self, publication: Any) -> str:
        filename = f"{self.generate_unique_key(self.INTERFACE_ID, publication, self.PUBLICATION_SALT)}_pub.json"
        self.logger.info("Fetching author publication %s", filename)
        pubfile: FileReader = FileReader(filename)

        pub_filled = scholarly.fill(publication)

        pubfile.dump_and_save(pub_filled)

        publication_id: str = publication['author_pub_id']
        citations_msg = GetScholarlyPubCitations(self.INTERFACE_ID + "_" + publication_id + "_citation",
                                                 publication_id, None, publication)
        MessageRouter.get_instance().send_later_in(citations_msg, self.INTERFACE_ID)

        articles_msg = GetScholarlyPubRelatedArticles(self.INTERFACE_ID + "_" + publication_id + "_article",
                                                      publication_id, None, publication)
        MessageRouter.get_instance().send_later_in(articles_msg, self.INTERFACE_ID)
        return filename
