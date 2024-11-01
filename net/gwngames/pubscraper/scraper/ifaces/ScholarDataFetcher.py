import logging
from datetime import datetime, timedelta
from typing import Final

from couchdb import Document

from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.FetchScholarData import FetchScholarlyData
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.scraper.ScholarScraper import ScholarScraper
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class ScholarDataFetcher(GeneralDataFetcher):
    CIT_ART_SALT: Final = ['bib.title']
    INTERFACE_ID: Final = 'google_scholar'

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=LoggingConstants.SCHOLARLY_DATA_FETCHER)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=ScholarDataFetcher.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), ScholarDataFetcher.INTERFACE_ID)

    def get_interface_id(self):
        return ScholarDataFetcher.INTERFACE_ID

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.get_interface_id())
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)
        if adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR:
            adapter.add_property(AdapterPropertiesConstants.IFACE_IS_ITERATOR, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().get_scholar_profile)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_COAUTHOR:
            adapter.add_property(AdapterPropertiesConstants.PHASE_REF, EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().get_scholar_profile)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_PUB:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().fetch_publication_data)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_CIT:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().scrape_all_citations)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        elif adapter_code == EntityCidConstants.GOOGLE_SCHOLAR_VERSION:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScholarScraper().scrape_all_versions)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Scholar - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, opt_arg)
        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})
        self.logger.info(f"Preparing next phase for phase_ref: {phase_ref}")

        if (phase_ref == EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR
                or phase_ref == EntityCidConstants.GOOGLE_SCHOLAR_COAUTHOR):
            self.logger.debug("Processing Google Scholar Author or Coauthor phase")
            publications = current_entity.get(JsonConstants.TAG_PUBLICATIONS, [])

            for pub in publications:
                self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_PUB,
                                                PriorityConstants.PUB_REQ, pub['url'], pub['publication_id'])

            coauthors = current_entity.get(JsonConstants.TAG_COAUTHORS, [])

            for coauthor in coauthors:
                self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_COAUTHOR,
                                                PriorityConstants.COAUTHOR_REQ, coauthor, coauthor)

        elif phase_ref == EntityCidConstants.GOOGLE_SCHOLAR_PUB:
            self.logger.debug("Processing Google Scholar Publication phase")
            cit_graph = current_entity.get(JsonConstants.TAG_CITATION_GRAPH, [])

            for citation_year in cit_graph:
                self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_CIT,
                                                PriorityConstants.CIT_REQ, citation_year['citation_link'],
                                                citation_year['citation_link'])

            all_versions_url = current_entity.get(JsonConstants.TAG_ALL_VERSIONS, [])
            self.generate_adapter_with_prio(EntityCidConstants.GOOGLE_SCHOLAR_VERSION,
                                            PriorityConstants.VERSION_REQ, all_versions_url, all_versions_url)

        self.logger.info("Completed preparing next phase for phase_ref: {phase_ref}")
        return super(ScholarDataFetcher, self).prepare_next_phase(phase_ref, current_entity)

    def get_key_fields(self, entity_cid: int) -> list[str]:
        if entity_cid == EntityCidConstants.GOOGLE_SCHOLAR_CIT or entity_cid == EntityCidConstants.GOOGLE_SCHOLAR_ART:
            return self.CIT_ART_SALT
        else:
            raise Exception("Scholarly - unknown iter cid: " + str(entity_cid))

    def _start_interface_collectors(self, opt_arg: list = None):
        MessageRouter.later_in(FetchScholarlyData(MessageConstants.MSG_ALL_SCHOLARLY_AUTHORS,
            self.generate_fetch_adapter(EntityCidConstants.GOOGLE_SCHOLAR_AUTHOR, opt_arg)),
            PriorityConstants.AUTHOR_REQ)

    def is_outdated(self, entity: Document):
        if entity is None or entity.get("update_date") is None:
            return True
        decoded_date = datetime.strptime(entity.get("update_date"), "%Y-%m-%d %H:%M:%S")
        time_difference = datetime.now() - decoded_date
        if abs(time_difference) > timedelta(hours=2):
            self.logger.info("[Scholar] Entity outdated: " + str(entity.get("_id")))
            return True
        return False

    def get_variant_type(self):
        return EntityVidConstants.SCHOLAR_VID
