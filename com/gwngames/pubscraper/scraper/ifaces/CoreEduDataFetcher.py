import logging
from typing import Final

from couchdb import Document

from com.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from com.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from com.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.scraper.FetchCoreEduData import FetchCoreEduData
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from com.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from com.gwngames.pubscraper.scraper.scraper.CoreEduScraper import CoreEduScraper
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class CoreEduDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'core_edu'

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)

    def generate_fetch_adapter(self, adapter_code: int) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.CONFERENCE:
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, CoreEduScraper().get_conferences_data)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Core - unknown adapter: " + str(adapter_code))

        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int, prev_adapter: GeneralDataAdapter) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})
        self.logger.info(f"No next phases for core")

        return self.adapter_list, self.priorities_map

    def _start_interface_collectors(self, opt_arg: list | int = None):
        for page in opt_arg:
            adapter = self.generate_fetch_adapter(EntityCidConstants.CONFERENCE)
            adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, int(page))
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST, [int(page)])
            MessageRouter.get_instance().send_message(FetchCoreEduData(MessageConstants.MSG_CORE_CONFERENCE,
                                                    adapter), PriorityConstants.CONFERENCE_REQ)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_key_fields(self, entity_cid: int) -> list[str]:
        raise Exception("Dblp - unknown iter cid: " + str(entity_cid))

    def get_variant_type(self) -> int:
        return EntityVidConstants.CORE_EDU_VID
