import logging
import threading
import time

from numpy import random

from com.gwngames.pubscraper.Context import Context
from com.gwngames.pubscraper.comm.OutSender import OutSender
from com.gwngames.pubscraper.comm.PackagingUnit import PackagingUnit
from com.gwngames.pubscraper.constants.ConfigConstants import ConfigConstants
from com.gwngames.pubscraper.msg.comm.PackageEntity import PackageEntity
from com.gwngames.pubscraper.msg.comm.SendEntity import SendEntity
from com.gwngames.pubscraper.scraper.NameFetcher import NameFetcher
from com.gwngames.pubscraper.scraper.buffer.DatabaseHandler import DatabaseHandler
from com.gwngames.pubscraper.scraper.ifaces.CoreEduDataFetcher import CoreEduDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.DblpDataFetcher import DblpDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.ScholarDataFetcher import ScholarDataFetcher
from com.gwngames.pubscraper.scraper.ifaces.ScimagoDataFetcher import ScimagoDataFetcher
from com.gwngames.pubscraper.utils.JsonReader import JsonReader
from com.gwngames.pubscraper.utils.StringUtils import StringUtils


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
        core_current_pages = int(config.get_value(ConfigConstants.CORE_PAGES_NUMBER))
        years = [str(year) for year in range(1999, __import__('datetime').datetime.now().year)]
        edu_pages = [str(i) for i in range(core_current_pages, 0, -1)]
        if author_from is not None:
            scraping_authors += NameFetcher.generate_roots(author_from)
        if root_authors is not None and root_authors != '':
            scraping_authors += StringUtils.process_string(root_authors)

        if config.get_value(ConfigConstants.SHUFFLE_ROOTS):
            random.shuffle(scraping_authors)

        # ---- RECOVERY ----
        if config.get_value(ConfigConstants.RECOVERY_INST) is True:
            WebScraper.recover_unsent_documents()
        # ------------------
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
                iface_instance.start_interface_fetching(opt_arg=edu_pages)


    @staticmethod
    def recover_unsent_documents():
        context = Context()
        db_client = context.get_dbclient()

        try:
            # Iterate over all databases in the CouchDB server
            for db_name in db_client:
                db = db_client[db_name]  # Access each database
                for doc_id in db:
                    doc = db[doc_id]
                    if doc.get('sent') is False:
                        entity_package_req: PackageEntity = PackageEntity("RECOVERY", doc_id, db_name)
                        entity_package_req.system_message = True
                        WebScraper.logger.debug(f"Sending ENTITY_PACKAGE_REQ for RECOVERED entity ID {doc_id}.")
                        compressed_data = PackagingUnit().compress_json(doc)
                        OutSender().send_data(SendEntity("RECOVERY", compressed_data, doc_id, db_name))
                        try:
                            DatabaseHandler(context.get_dbclient(), db_name).insert_or_update_document(doc['type'], doc_id, doc)
                        except Exception:
                            continue
                        time.sleep(1)
        except Exception as e:
            context.logger.error(f"Error recovering unsent documents: {e}")

