import logging
from typing import Final

from couchdb import Document

from net.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from net.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from net.gwngames.pubscraper.constants.LoggingConstants import LoggingConstants
from net.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from net.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from net.gwngames.pubscraper.msg.scraper.FetchCoreEduData import FetchCoreEduData
from net.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from net.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from net.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from net.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from net.gwngames.pubscraper.scraper.scraper.CoreEduScraper import CoreEduScraper
from net.gwngames.pubscraper.utils.JsonReader import JsonReader


class CoreEduDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'core_edu'

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        logging.basicConfig(level=LoggingConstants.CORE_EDU_DATA_FETCHER)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def generate_fetch_adapter(self, adapter_code: int, opt_arg: list | int = None) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.CONFERENCE:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, CoreEduScraper().get_conferences_data)
        else:
            raise Exception("Dblp - unknown adapter: " + str(adapter_code))

        if opt_arg is not None:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM, str(opt_arg))
            adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, str(opt_arg))
        return adapter

    page_fallback = 1

    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})

        if phase_ref == EntityCidConstants.CONFERENCE and current_entity:
            self.logger.debug("Processing Scimago Journals next phase")

            page_number = current_entity.get("page_number", CoreEduDataFetcher.page_fallback)
            CoreEduDataFetcher.page_fallback += 1

            self.generate_adapter_with_prio(EntityCidConstants.CONFERENCE,
                                            PriorityConstants.CONFERENCE_REQ, page_number, str(page_number))

        self.logger.info("Completed preparing next phase for phase_ref: {phase_ref}")

        # Message is re executed
        return super(CoreEduDataFetcher, self).prepare_next_phase(phase_ref, current_entity, phase_depth)

    def _start_interface_collectors(self, opt_arg: list | int = None):
        MessageRouter.later_in(FetchCoreEduData(MessageConstants.MSG_ALL_CORE_CONFERENCE,
                                                self.generate_fetch_adapter(EntityCidConstants.CONFERENCE, opt_arg)),
                               PriorityConstants.CONFERENCE_REQ)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_key_fields(self, entity_cid: int) -> list[str]:
        raise Exception("Dblp - unknown iter cid: " + str(entity_cid))

    def get_variant_type(self) -> int:
        return EntityVidConstants.CORE_EDU_VID
