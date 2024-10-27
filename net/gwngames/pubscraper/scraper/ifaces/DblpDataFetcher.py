import logging
from typing import Final

from couchdb import Document

from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.FetchDblpData import FetchDblpData
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.CoreEduDataFetcher import CoreEduDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.ifaces.ScimagoDataFetcher import ScimagoDataFetcher
from net.gwngames.pubscraper.scraper.scraper.DblpScraper import DblpScraper
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class DblpDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'dblp'

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=LoggingConstants.DBLP_DATA_FETCHER)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list = None) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.DBLP_PUBS:
            adapter.add_property(AdapterPropertiesConstants.IFACE_IS_ITERATOR, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, DblpScraper().get_author_publications)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        elif adapter_code == EntityCidConstants.DBLP_JOURNAL:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, DblpScraper().get_journal_volume_data)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        elif adapter_code == EntityCidConstants.DBLP_CONFERENCE:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, DblpScraper().extract_articles)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Dblp - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, opt_arg)
        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})

        self.logger.info(f"Preparing next phase for phase_ref: {phase_ref}")

        if phase_ref == EntityCidConstants.DBLP_PUBS:
            self.logger.debug("Processing Dblp publications phase")
            publications = current_entity.get(JsonConstants.TAG_PUBLICATIONS, [])

            for pub in publications:
                if pub[JsonConstants.TAG_TYPE] == "Journal":
                    self.generate_adapter_with_prio(EntityCidConstants.DBLP_JOURNAL,
                                                    PriorityConstants.JOURNAL_REQ, pub['link'], pub['link'])
                elif pub[JsonConstants.TAG_TYPE] == "Conference":
                    self.generate_adapter_with_prio(EntityCidConstants.DBLP_CONFERENCE,
                                                    PriorityConstants.CONFERENCE_REQ, pub["link"], pub["link"])
                else:
                    self.logger.warning("Ignoring: " + pub[JsonConstants.TAG_TYPE])
        elif phase_ref == EntityCidConstants.DBLP_JOURNAL:
            self.logger.debug("Processing Dblp journal phase")
            journals = current_entity.get(JsonConstants.TAG_JOURNALS, [])

            for journal in journals:
                self.generate_adapter_with_prio(EntityCidConstants.SCIMAGO_JOURNAL_RANK, PriorityConstants.JOURNAL_RANK_REQ,
                                                journal['collection_title'], journal['collection_title'], ScimagoDataFetcher())

        elif phase_ref == EntityCidConstants.DBLP_CONFERENCE:
            self.logger.debug("Processing Dblp conference phase")
            conferences = current_entity.get(JsonConstants.TAG_CONFERENCES, [])

            for conf in conferences:
                acronyms = conf['conferences']
                for acronym in acronyms:
                    self.generate_adapter_with_prio(EntityCidConstants.CORE_MAIN_CONFERENCE_RANK,
                                                    PriorityConstants.CONFERENCE_MAIN_RANK_REQ,
                                                    acronym, acronym,
                                                    CoreEduDataFetcher())
                break  # only first record if exists

        self.logger.info("Completed preparing next phase for phase_ref: {phase_ref}")
        return super(DblpDataFetcher, self).prepare_next_phase(phase_ref, current_entity)

    def _start_interface_collectors(self, opt_arg: list = None):
        MessageRouter.later_in(FetchDblpData(MessageConstants.MSG_ALL_DBLP_AUTHORS,
                                             self.generate_fetch_adapter(EntityCidConstants.DBLP_PUBS, opt_arg)),
                               PriorityConstants.AUTHOR_REQ)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_key_fields(self, entity_cid: int) -> list[str]:
        raise Exception("Dblp - unknown iter cid: " + str(entity_cid))

    def get_variant_type(self) -> int:
        return EntityVidConstants.DBLP_VID
