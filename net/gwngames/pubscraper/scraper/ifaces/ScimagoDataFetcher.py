import logging
from typing import Final

from couchdb import Document

from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from net.gwngames.pubscraper.constants.JsonConstants import JsonConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.FetchScimagoData import FetchScimagoData
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.scraper.ScimagoScraper import ScimagoScraper
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class ScimagoDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'scimago'

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

        if adapter_code == EntityCidConstants.JOURNAL:
            adapter.add_property(AdapterPropertiesConstants.IFACE_IS_ITERATOR, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScimagoScraper().get_journals_from_page)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Dblp - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.ALT_ITERABLE, opt_arg)
        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})

        if phase_ref == EntityCidConstants.JOURNAL:
            self.logger.debug("Processing Scimago Journals next phase")

            is_end = current_entity.get(JsonConstants.TAG_IS_END, False)  # Better running than dead, pace is defined
            area = current_entity.get("area", "")  # Default to void

            if not is_end:
                self.generate_adapter_with_prio(EntityCidConstants.JOURNAL,
                                                PriorityConstants.JOURNAL_REQ, area, area + str(ScimagoScraper.year))

        self.logger.info("Completed preparing next phase for phase_ref: {phase_ref}")

        # Message is re executed
        return super(ScimagoDataFetcher, self).prepare_next_phase(phase_ref, current_entity)

    def _start_interface_collectors(self, opt_arg: list | int = None):
        MessageRouter.later_in(FetchScimagoData(MessageConstants.MSG_ALL_SCIMAGO_JOURNALS,
                                                self.generate_fetch_adapter(EntityCidConstants.JOURNAL, opt_arg)),
                               PriorityConstants.JOURNAL_REQ)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_key_fields(self, entity_cid: int) -> list[str]:
        raise Exception("Dblp - unknown iter cid: " + str(entity_cid))

    def get_variant_type(self) -> int:
        return EntityVidConstants.SCIMAGO_VID
