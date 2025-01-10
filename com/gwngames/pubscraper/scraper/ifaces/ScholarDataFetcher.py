import logging
import time
from typing import Final, Set

from couchdb import Document

from com.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from com.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from com.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.scraper.FetchScholarData import FetchScholarlyData
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from com.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from com.gwngames.pubscraper.scraper.scraper.ScholarScraper import ScholarScraper
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class ScholarDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'google_scholar'

    PUB_AUTHORS: Set = set()

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=ScholarDataFetcher.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), ScholarDataFetcher.INTERFACE_ID)

    def get_interface_id(self):
        return ScholarDataFetcher.INTERFACE_ID

    def generate_fetch_adapter(self, adapter_code: int) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.get_interface_id())
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)
        if adapter_code == EntityCidConstants.AUTHOR:
            adapter.add_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, False)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().get_scholar_profile)
        elif adapter_code == EntityCidConstants.PUB:
            adapter.add_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().fetch_publication_data)
        elif adapter_code == EntityCidConstants.CIT:
            adapter.add_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().scrape_all_citations)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        elif adapter_code == EntityCidConstants.VERSION:
            adapter.add_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().scrape_all_versions)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Scholar - unknown adapter: " + str(adapter_code))

        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int, prev_adapter: GeneralDataAdapter) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})
        self.logger.info(f"Preparing next phase from: {prev_adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID)}")

        if phase_ref == EntityCidConstants.AUTHOR:
            self.logger.debug("Processing Google Scholar Author or Coauthor phase")

            publications = current_entity.get("publications", [])

            self.logger.info(f"Found {len(publications)} publications")

            for pub in publications:
                self.generate_adapter_with_prio(EntityCidConstants.PUB,
                                                PriorityConstants.PUB_REQ, [pub['url']], pub['publication_id'])

            coauthors = current_entity.get("coauthors", [])

            self.logger.info(f"Found {len(coauthors)} coauthors")
            for coauthor in coauthors:
                if coauthor not in ScholarDataFetcher.PUB_AUTHORS:
                    self.generate_adapter_with_prio(EntityCidConstants.AUTHOR,
                                                PriorityConstants.AUTHOR_REQ, [coauthor], coauthor)

        elif phase_ref == EntityCidConstants.PUB:
            self.logger.debug("Processing Google Scholar Publication phase")
            cit_graph = current_entity.get("citation_graph", [])
            authors = current_entity.get("authors")
            pub_id = current_entity.get("publication_id")

            for author in authors:
                if author not in ScholarDataFetcher.PUB_AUTHORS:
                    self.generate_adapter_with_prio(EntityCidConstants.AUTHOR,
                                                PriorityConstants.AUTHOR_REQ, [author], author)
                    ScholarDataFetcher.PUB_AUTHORS.add(author)

           # for citation_year in cit_graph:
            #    self.generate_adapter_with_prio(EntityCidConstants.CIT,
             #                                   PriorityConstants.CIT_REQ * 10, citation_year['citation_link'],
              #                                  citation_year['citation_link'], opt_param=pub_id)

            #all_versions_url = current_entity.get(JsonConstants.TAG_ALL_VERSIONS, [])
           # self.generate_adapter_with_prio(EntityCidConstants.VERSION,
            #                                PriorityConstants.VERSION_REQ*10, all_versions_url, all_versions_url)

        self.logger.info(f"Completed preparing next phase from: {prev_adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID)}")

        return super(ScholarDataFetcher, self).prepare_next_phase(phase_ref, current_entity, phase_depth, prev_adapter)

    def _start_interface_collectors(self, opt_arg: list):
        for author in opt_arg:
            author_adapter = self.generate_fetch_adapter(EntityCidConstants.AUTHOR)
            author_adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST, [author])
            author_adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, author)
            MessageRouter.get_instance().send_message(FetchScholarlyData(MessageConstants.MSG_SCHOLARLY_AUTHOR,
                author_adapter),
                PriorityConstants.AUTHOR_REQ)
            time.sleep(30)

    def get_variant_type(self):
        return EntityVidConstants.SCHOLAR_VID


