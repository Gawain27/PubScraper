import logging
import random
from typing import Final, Tuple, List, Any, Dict

from couchdb import ResourceNotFound, ServerError, Unauthorized, ResourceConflict, Server, Document
from scholarly import ProxyGenerator
from scholarly import scholarly

from net.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.scholarly.FetchScholarlyData import FetchScholarlyData
from net.gwngames.pubscraper.msg.scraper.scholarly.GetAllScholarlyAuthors import GetAllScholarlyAuthors
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyAuthor import GetScholarlyAuthor
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyOrg import GetScholarlyOrg
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubCitations import GetScholarlyPubCitations
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPubRelatedArticles import GetScholarlyPubRelatedArticles
from net.gwngames.pubscraper.msg.scraper.scholarly.GetScholarlyPublication import GetScholarlyPublication
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.utils.JsonReader import JsonReader
from net.gwngames.pubscraper.utils.ThreadUtils import ThreadUtils


class ScholarlyDataFetcher(GeneralDataFetcher):
    BASE_URL: Final = "https://scholar.google.com"
    PUBLICATION_SALT: Final = ['author_pub_id', 'bib.pub_year', 'bib.title']
    CIT_ART_SALT: Final = ['bib.title']
    AUTHOR_SALT: Final = ['scholar_id', 'name']
    PROXY_STARTED: bool = False
    INTERFACE_ID: Final = 'google_scholar'

    def __init__(self, proxy: bool = True):
        super().__init__()  # Assuming GeneralDataFetcher is a parent class
        self.proxy_enabled = proxy

        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=LoggingConstants.SCHOLARLY_DATA_FETCHER)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)

        if self.proxy_enabled and not ScholarlyDataFetcher.PROXY_STARTED:
            self.refresh_proxy()
            ScholarlyDataFetcher.PROXY_STARTED = True

        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def get_or_create_db(self, client: Server, db_name):
        try:
            db = client[db_name]
        except ResourceNotFound:
            self.logger.info(f"Database {db_name} not found. Creating new database.")
            db = client.create(db_name)
        except Unauthorized:
            self.logger.error("Unauthorized access to CouchDB. Check your credentials.")
            raise
        except ServerError as e:
            self.logger.error(f"Server error: {e}")
            raise
        return db

    def refresh_proxy(self):
        pg = ProxyGenerator()
        key: str = self.config.get_value(ConfigConstants.SCRAPERAPI_KEY)
        if key is None:
            raise Exception("A Scraper API key must be provided for scholarly to work!")
        else:
            success = pg.ScraperAPI(key)
        if not success:
            # opt for free proxy, double the slowness of the process
            success = pg.FreeProxies(1, random.uniform(self.config.get_value(ConfigConstants.MIN_WAIT_TIME)*2,
                                             self.config.get_value(ConfigConstants.MAX_WAIT_TIME)*2))
        if not success:
            raise Exception("Couldn't set up proxy for scraping")
        scholarly.use_proxy(pg)

    def fetch_multiple_authors(self, authors_list: list, depth: int = None):
        self.logger.info("Fetching all relevant authors: %s", authors_list)

        authors: list[str] = authors_list
        for author in authors:
            author_msg = GetScholarlyAuthor(self.INTERFACE_ID + "_" + author, author)
            MessageRouter.get_instance().send_later_in(author_msg,
                                                       depth=depth, priority=PriorityConstants.AUTHOR_REQ)
            self.logger.info("Sent message %s for author: %s", author_msg.message_id, author)
            ThreadUtils.deschedule(self.config, "fetch_all_authors")

    def insert_or_update_document(self, doc_type, doc_id, doc):
        doc['_id'] = doc_id
        doc['type'] = doc_type
        doc['serialized'] = False
        doc['fetching_completed'] = False
        try:
            existing_doc = self.db.get(doc_id)
            if existing_doc:
                doc['_rev'] = existing_doc['_rev']
            self.db.save(doc)
            self.logger.info(f"Document of type {doc_type} with id {doc_id} saved successfully.")
        except ResourceConflict:
            self.logger.warning(
                f"Conflict encountered while saving document of type {doc_type} with id {doc_id}. Retrying.")
            self.insert_or_update_document(doc_type, doc_id, doc)
        except Exception as e:
            self.logger.error(f"Error saving document: {e}")

    def fetch_author_data(self, msg: GetScholarlyAuthor) -> str:
        search_query = scholarly.search_author(msg.author)
        existing_author = self.db.get(f"author_{msg.author}")

        if not existing_author or self.is_outdated(existing_author):
            author_snip = next(search_query)
            full_author = scholarly.fill(author_snip)
            self.logger.info("Retrieved author: %s", msg.author)
            self.insert_or_update_document('author', f"author_{full_author['name']}", full_author)
        else:
            full_author = existing_author
            self.logger.info("Loaded up to date author %s", msg.author)

        # Fetch coauthors
        coauthors = [item['name'] for item in full_author.get(JsonConstants.TAG_COAUTHORS, [])]
        msg_coauthors = GetAllScholarlyAuthors(f"{self.INTERFACE_ID}_coauthors_{msg.author}.json", coauthors)
        MessageRouter.get_instance().send_later_in(msg_coauthors, depth=msg.depth,
                                                   priority=PriorityConstants.COAUTHOR_REQ)

        org_hint: str = full_author.get(JsonConstants.TAG_EMAIL_DOMAIN)
        if org_hint and len(org_hint.split('.')) >= 2:
            org = org_hint.split('.')[-2]
        else:
            self.logger.warning("No organizations found for %s", msg.author)
            return JsonReader.DEV_NULL

        existing_org = self.db.get(f"organization_{org}")
        if not existing_org or self.is_outdated(existing_org):
            msg_org = GetScholarlyOrg(f"{self.INTERFACE_ID}_{org}.json", org)
            MessageRouter.get_instance().send_message(msg_org, priority=PriorityConstants.ORG_REQ)

        pub_number: int = 0
        for pub in full_author.get('publications', []):
            pub_id = f"publication_{pub['author_pub_id']}"
            existing_pub = self.db.get(pub_id)

            if pub_number >= self.config.get_value(ConfigConstants.MAX_FETCHABLE):
                self.logger.info("Scraped all publications for this run for Author: %s", msg.author)
                break

            if existing_pub and not self.is_outdated(existing_pub):
                self.logger.info("Publication %s is up to date", pub_id)
                publication_id: str = pub['author_pub_id']
                citations_msg = GetScholarlyPubCitations(self.INTERFACE_ID + "_" + publication_id + "_citation",
                                                         publication_id, None, existing_pub)
                MessageRouter.get_instance().send_later_in(citations_msg, priority=PriorityConstants.CIT_REQ)

                articles_msg = GetScholarlyPubRelatedArticles(self.INTERFACE_ID + "_" + publication_id + "_article",
                                                              publication_id, None, existing_pub)
                MessageRouter.get_instance().send_later_in(articles_msg, priority=PriorityConstants.REL_ART_REQ)
                ThreadUtils.deschedule(self.ctx.get_config(), publication_id+"_article")
                continue

            pub_number += 1

            publication_msg = GetScholarlyPublication(pub_id, pub)
            MessageRouter.get_instance().send_later_in(publication_msg, priority=PriorityConstants.PUB_REQ)
            self.logger.info("Sent publication message for pub %s for author %s", pub_id, msg.author)
            ThreadUtils.deschedule(self.ctx.get_config(), pub_id)

        return full_author.get('name', JsonReader.DEV_NULL)

    def fetch_org_data(self, msg):
        org_name = msg.org_name

        possible_orgs = scholarly.search_org(org_name)
        org_fetched = False
        org_key = None
        if possible_orgs and len(possible_orgs) > 0:
            actual_org = possible_orgs[0]
            org_key = actual_org.get('id')

            existing_org = self.db.get(f'organization_{org_key}')

            if existing_org and not self.is_outdated(existing_org):
                return JsonReader.DEV_NULL

            self.insert_or_update_document('organization', f"organization_{org_key}", actual_org)
            self.logger.info("Saved organization %s to database", org_name)
            org_fetched = True

        return f'organization_{org_key}' if org_fetched else JsonReader.DEV_NULL

    pub_cit_num: dict[str, int] = {}
    cit_starting_index: dict[str, int] = {}

    def fetch_pub_citations(self, msg: GetScholarlyPubCitations) -> str:
        unique_citation_key = f"{msg.pub_id}_citation"
        citations_record = self.db.get(unique_citation_key)

        if msg.citations is None:
            assert msg.pub is not None
            citations = scholarly.citedby(msg.pub)
            MessageRouter.get_instance().send_later_in(
                GetScholarlyPubCitations(unique_citation_key, msg.pub_id, citations), priority=PriorityConstants.CIT_REQ)
            self.pub_cit_num[unique_citation_key] = 0
            startingIndex = JsonReader(JsonReader.MESSAGE_STAT_FILE_NAME).get_value(msg.content)[0]
            self.cit_starting_index[unique_citation_key] = int(startingIndex) if startingIndex is not None else 0
            self.logger.info("Scraping citations for %s starting at index %d", msg.content,
                             self.cit_starting_index[unique_citation_key])
            return JsonReader.DEV_NULL

        self.logger.info("Fetching citation %s - %s", self.pub_cit_num[unique_citation_key], msg.pub_id)

        if self.pub_cit_num[unique_citation_key] > self.config.get_value(ConfigConstants.MAX_FETCHABLE):
            self.logger.warning("Reached max number of citations fetchable for %s", unique_citation_key)
            return JsonReader.DEV_NULL

        citation = next(msg.citations, self.cit_starting_index.get(unique_citation_key, None))
        if self.cit_starting_index.get(unique_citation_key, None) is not None:
            self.cit_starting_index[unique_citation_key] = None

        unique_citation = self.generate_unique_key("", citation, self.CIT_ART_SALT, limit=True)
        if not citations_record:
            citations_record = {'_id': unique_citation_key, 'type': 'citation', 'citations': []}

        if unique_citation not in citations_record['citations']:
            citations_record['citations'].append(unique_citation)
            self.insert_or_update_document('citation', unique_citation_key, citations_record)
            self.logger.info("Saved citation %s to database", self.pub_cit_num[unique_citation_key])
            self.pub_cit_num[unique_citation_key] += 1
        else:
            self.logger.info("Citation %s is up to date", unique_citation)

        MessageRouter.get_instance().send_later_in(
            GetScholarlyPubCitations(unique_citation_key, msg.pub_id, msg.citations), priority=PriorityConstants.CIT_REQ)
        ThreadUtils.deschedule(self.ctx.get_config(), unique_citation_key)

        return unique_citation_key

    pub_art_num: dict[str, int] = {}
    art_starting_index: dict[str, int] = {}

    def fetch_related_articles(self, msg: GetScholarlyPubRelatedArticles) -> str:
        unique_article_key = f"{msg.pub_id}_related"
        related_articles_record = self.db.get(unique_article_key)

        if msg.articles is None:
            assert msg.pub is not None
            articles = scholarly.get_related_articles(msg.pub)
            MessageRouter.get_instance().send_later_in(
                GetScholarlyPubRelatedArticles(unique_article_key, msg.pub_id, articles), priority=PriorityConstants.REL_ART_REQ)
            self.pub_art_num[unique_article_key] = 0
            startingIndex = JsonReader(JsonReader.MESSAGE_STAT_FILE_NAME).get_value(msg.content)[0]
            self.art_starting_index[unique_article_key] = int(startingIndex) if startingIndex is not None else 0
            self.logger.info("Scraping articles for %s starting at index %d", msg.content,
                             self.art_starting_index[unique_article_key])
            return JsonReader.DEV_NULL

        self.logger.info("Fetching article %s - %s", self.pub_art_num[unique_article_key], msg.pub_id)

        if self.pub_art_num[unique_article_key] > self.config.get_value(ConfigConstants.MAX_FETCHABLE):
            self.logger.warning("Reached max number of articles fetchable for %s", unique_article_key)
            return JsonReader.DEV_NULL

        article = next(msg.articles, self.art_starting_index.get(unique_article_key, None))
        if self.art_starting_index.get(unique_article_key, None) is not None:
            self.art_starting_index[unique_article_key] = None

        unique_article = self.generate_unique_key(article, self.CIT_ART_SALT)
        if not related_articles_record or unique_article not in related_articles_record.get('articles', []):
            if not related_articles_record:
                related_articles_record = {'_id': unique_article_key, 'type': 'related_articles', 'articles': []}
            related_articles_record['articles'].append(unique_article)
            self.insert_or_update_document('related_articles', unique_article_key, related_articles_record)
            self.logger.info("Saved article %s to database", unique_article)
            self.pub_art_num[unique_article_key] += 1
        else:
            self.logger.info("Article %s is up to date", unique_article)

        MessageRouter.get_instance().send_later_in(
            GetScholarlyPubRelatedArticles(unique_article_key, msg.pub_id, msg.articles), priority=PriorityConstants.REL_ART_REQ)
        ThreadUtils.deschedule(self.ctx.get_config(), unique_article_key)

        return unique_article_key

    def fetch_author_publication(self, msg: GetScholarlyPublication) -> str:
        self.logger.info("Fetching author publication %s", msg.content)
        unique_publication_key = msg.pub['author_pub_id']

        existing_pub = self.db.get(unique_publication_key)

        if existing_pub and self.is_outdated(existing_pub):
            pub_filled = scholarly.fill(msg.pub)
            self.insert_or_update_document('publication', unique_publication_key, pub_filled)
            self.logger.info("Updated publication %s in database", unique_publication_key)
        else:
            pub_filled = scholarly.fill(msg.pub)
            self.insert_or_update_document('publication', unique_publication_key, pub_filled)
            self.logger.info("Inserted new publication %s into database", unique_publication_key)

        publication_id = msg.pub['author_pub_id']
        citations_msg = GetScholarlyPubCitations(f"{self.INTERFACE_ID}_{publication_id}_citation",
                                                 publication_id, None, pub_filled)
        MessageRouter.get_instance().send_later_in(citations_msg, priority=PriorityConstants.CIT_REQ)

        articles_msg = GetScholarlyPubRelatedArticles(f"{self.INTERFACE_ID}_{publication_id}_article",
                                                      publication_id, None, pub_filled)
        MessageRouter.get_instance().send_later_in(articles_msg, priority=PriorityConstants.REL_ART_REQ)
        ThreadUtils.deschedule(self.ctx.get_config(), publication_id)

        return unique_publication_key

    def get_interface_id(self):
        return self.INTERFACE_ID

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)
        if adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, scholarly.search_author)
            adapter.add_property(AdapterPropertiesConstants.IFACE_ADDITIONAL_FX, scholarly.fill)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_PUB:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, scholarly.fill)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_ORG:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, scholarly.search_org)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_CIT:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, scholarly.citedby)
            adapter.add_property(AdapterPropertiesConstants.IFACE_IDX, True)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_ART:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, scholarly.get_related_articles)
            adapter.add_property(AdapterPropertiesConstants.IFACE_IDX, True)
        else:
            raise Exception("Scholarly - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, opt_arg)
        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document) -> tuple[list[GeneralDataAdapter], dict]:

        if phase_ref == EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR:
            coauthors = [item['name'] for item in current_entity.get(JsonConstants.TAG_COAUTHORS, [])]
            org_hint: str = current_entity.get(JsonConstants.TAG_EMAIL_DOMAIN)
            org: str = ''
            if org_hint and len(org_hint.split('.')) >= 2:
                org = org_hint.split('.')[-2]

            publications = [pub[JsonConstants.TAG_PUB_ID] for pub in current_entity.get(JsonConstants.TAG_PUBLICATIONS, [])]

            self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_PUB, PriorityConstants.PUB_REQ, publications)
            self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_COAUTHORS, PriorityConstants.COAUTHOR_REQ, coauthors)
            self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_ORG, PriorityConstants.ORG_REQ, self.get_interface_id() + org)
        elif phase_ref == EntityCidConstants.GOOGLE_SCHOLAR_PUB:
            None
            #self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_CIT, PriorityConstants.CIT_REQ, current_entity)
            #self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_ART, PriorityConstants.REL_ART_REQ, current_entity)

        return super(ScholarlyDataFetcher, self).prepare_next_phase(phase_ref, current_entity)

    def get_key_fields(self, entity_cid: int) -> list[str]:
        if entity_cid == EntityCidConstants.GOOGLE_SCHOLAR_CIT or entity_cid == EntityCidConstants.GOOGLE_SCHOLAR_ART:
            return self.CIT_ART_SALT
        else:
            raise Exception("Scholarly - unknown iter cid: " + str(entity_cid))

    def _start_interface_collectors(self, opt_arg: list = None):
        MessageRouter.later_in(FetchScholarlyData(
            self.generate_fetch_adapter(EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR, opt_arg)),
            PriorityConstants.AUTHOR_REQ)



