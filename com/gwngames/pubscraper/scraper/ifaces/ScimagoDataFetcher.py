import logging
from typing import Final

from couchdb import Document

from com.gwngames.pubscraper.constants.EntityCidConstants import EntityCidConstants
from com.gwngames.pubscraper.constants.EntityVidConstants import EntityVidConstants
from com.gwngames.pubscraper.constants.MessageConstants import MessageConstants
from com.gwngames.pubscraper.constants.PriorityConstants import PriorityConstants
from com.gwngames.pubscraper.msg.scraper.FetchScimagoData import FetchScimagoData
from com.gwngames.pubscraper.scheduling.MessageRouter import MessageRouter
from com.gwngames.pubscraper.scraper.adapter.AdapterPropertiesConstants import AdapterPropertiesConstants
from com.gwngames.pubscraper.scraper.adapter.GeneralDataAdapter import GeneralDataAdapter
from com.gwngames.pubscraper.scraper.ifaces.GeneralDataFetcher import GeneralDataFetcher
from com.gwngames.pubscraper.scraper.scraper.ScimagoScraper import ScimagoScraper
from com.gwngames.pubscraper.utils.JsonReader import JsonReader


class ScimagoDataFetcher(GeneralDataFetcher):
    INTERFACE_ID: Final = 'scimago'

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.config = JsonReader(JsonReader.CONFIG_FILE_NAME, parent=self.INTERFACE_ID)
        self.db = self.get_or_create_db(self.ctx.get_dbclient(), self.INTERFACE_ID)


    def generate_fetch_adapter(self, adapter_code: int) -> GeneralDataAdapter:
        adapter: GeneralDataAdapter = GeneralDataAdapter()
        adapter.add_property(AdapterPropertiesConstants.IFACE_REF, self.INTERFACE_ID)
        adapter.add_property(AdapterPropertiesConstants.PHASE_REF, adapter_code)

        if adapter_code == EntityCidConstants.JOURNAL:
            adapter.add_property(AdapterPropertiesConstants.ROLL_OVER_DEPTH, True)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX, ScimagoScraper().get_journals_from_page)
            adapter.add_property(AdapterPropertiesConstants.MULTI_RESULT, True)
        else:
            raise Exception("Scimago - unknown adapter: " + str(adapter_code))

        return adapter

    def prepare_next_phase(self, phase_ref: int, current_entity: Document, phase_depth: int, prev_adapter: GeneralDataAdapter) -> tuple[list[GeneralDataAdapter], dict]:
        self.adapter_list, self.priorities_map = ([], {})

        if phase_ref == EntityCidConstants.JOURNAL:
            self.logger.debug("Processing Scimago Journals next phase from: %s", prev_adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID))

            if not current_entity.get("is_end"):
                next_page = self.ctx.get_message_data().increment("scimago_year_" + prev_adapter.get_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST)[0])
                next_page = str(next_page)
                param_list = prev_adapter.get_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST)
                param_list[1] = next_page
                self.generate_adapter_with_prio(EntityCidConstants.JOURNAL,
                                                PriorityConstants.JOURNAL_REQ, param_list,
                                                param_list[0]+"_"+next_page)

        self.logger.info(f"Completed preparing next phase from: {prev_adapter.get_property(AdapterPropertiesConstants.EXPECTED_ID)}.")

        # Message is re executed
        return super(ScimagoDataFetcher, self).prepare_next_phase(phase_ref, current_entity, phase_depth, prev_adapter)

    def _start_interface_collectors(self, opt_arg: list):
        for year in opt_arg:
            if self.ctx.get_message_data().get_value("scimago_year_" + year) is None:
                self.ctx.get_message_data().set_value("scimago_year_" + year, 1)

            adapter = self.generate_fetch_adapter(EntityCidConstants.JOURNAL)
            adapter.add_property(AdapterPropertiesConstants.IFACE_FX_PARAM_LIST, [year, 1])
            adapter.add_property(AdapterPropertiesConstants.EXPECTED_ID, year+"_1")

            MessageRouter.get_instance().send_message(FetchScimagoData(MessageConstants.MSG_SCIMAGO_JOURNAL, adapter),
                                   PriorityConstants.JOURNAL_REQ)

    def get_interface_id(self) -> str:
        return self.INTERFACE_ID

    def get_variant_type(self) -> int:
        return EntityVidConstants.SCIMAGO_VID
